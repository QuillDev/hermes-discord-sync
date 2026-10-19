"""Gateway half: hand the sync forum and @mentions to the backend bridge while it is alive."""

import contextlib
import logging
import time

log = logging.getLogger("hermes.discord-sync")

# The bridge touches the heartbeat every 2 seconds. When the backend is down,
# the normal gateway answers forum posts instead of leaving them unanswered.
FRESH_SECONDS = 20
INSTALLED = "_hermes_discord_sync_forum"
SEEN = "👀"
BUTTON_PREFIX = "hds:"
OFFLINE = "discord sync is offline right now. try again in a moment, or answer in the desktop app."


def _sync_dir():
    from hermes_constants import get_hermes_home

    return get_hermes_home() / "discord-sync"


def _bridge_alive():
    try:
        changed = (_sync_dir() / "heartbeat").stat().st_mtime
    except OSError:
        return False
    return time.time() - changed < FRESH_SECONDS


def _push(event):
    from .dashboard.sync.inbox import Inbox

    Inbox(_sync_dir() / "inbox").put(event)


def _allowed_users():
    from agent.secret_scope import UnscopedSecretError, build_profile_secret_scope, get_secret
    from hermes_constants import get_hermes_home

    try:
        raw = get_secret("DISCORD_ALLOWED_USERS") or ""
    except UnscopedSecretError:
        # Discord events arrive outside any turn, so a multiplexed gateway has
        # no profile scope installed; read this profile's own secrets instead.
        raw = build_profile_secret_scope(get_hermes_home()).get("DISCORD_ALLOWED_USERS") or ""
    return {user.strip() for user in raw.split(",") if user.strip().isdecimal()}


def _message_dict(message):
    """The message as the Discord REST API would return it, so the bridge has one format."""
    author = message.author
    webhook = getattr(message, "webhook_id", None)
    attachments = [
        {"url": item.url, "size": item.size, "filename": item.filename, "content_type": item.content_type}
        for item in getattr(message, "attachments", [])
    ]
    return {
        "id": str(message.id),
        "content": message.content or "",
        "author": {"id": str(author.id), "bot": bool(getattr(author, "bot", False))},
        "webhook_id": str(webhook) if webhook else None,
        "attachments": attachments,
    }


def _is_sync_mention(bot, message):
    """An @mention of the bot, in a server text channel, from an allowed user."""
    bot_user = getattr(bot, "user", None)
    if bot_user is None or bot_user not in getattr(message, "mentions", []):
        return False
    # Threads, forums and DMs keep the normal bot behavior.
    in_text_channel = (
        getattr(message, "guild", None) is not None and type(message.channel).__name__ == "TextChannel"
    )
    from_person = not message.author.bot
    return in_text_channel and from_person and str(message.author.id) in _allowed_users()


def _from_allowed_person(message):
    author = message.author
    return not getattr(author, "bot", False) and str(author.id) in _allowed_users()


async def _seen(message):
    """React the moment the message arrives, as the native bot does; the bridge takes it from here."""
    with contextlib.suppress(Exception):
        await message.add_reaction(SEEN)


async def _claim(bot, message, forum_id):
    """Push a message to the bridge when it owns it. False leaves it to the normal bot."""
    if not _bridge_alive():
        return False
    channel = message.channel
    if str(getattr(channel, "parent_id", "")) == forum_id:
        thread = {"thread": str(channel.id), "name": getattr(channel, "name", None)}
        _push({"kind": "forum", **thread, "message": _message_dict(message)})
        if _from_allowed_person(message):
            await _seen(message)
        return True
    if _is_sync_mention(bot, message):
        _push({"kind": "mention", "channel": str(channel.id), "message": _message_dict(message)})
        await _seen(message)
        return True
    return False


async def _press(interaction):
    """Acknowledge one of our buttons within Discord's 3 seconds, then hand it to the bridge."""
    data = getattr(interaction, "data", None) or {}
    custom_id = str(data.get("custom_id", ""))
    if not custom_id.startswith(BUTTON_PREFIX):
        return
    user = getattr(interaction, "user", None)
    if not _bridge_alive():
        with contextlib.suppress(Exception):
            await interaction.response.send_message(OFFLINE, ephemeral=True)
        return
    # The bridge checks the allowlist and edits the card to show the outcome.
    _push(
        {
            "kind": "press",
            "custom_id": custom_id,
            "user": str(getattr(user, "id", "")),
            "name": getattr(user, "display_name", None),
        }
    )
    try:
        await interaction.response.defer()
    except Exception:
        log.debug("Discord sync could not acknowledge a button press", exc_info=True)


def register(ctx):
    forum_id = str(ctx.get_config("forum_id", ""))
    if not forum_id.isdecimal():
        return

    def install(bot, adapter):
        # Native discord.py event registration is the documented plugin seam.
        if getattr(bot, INSTALLED, None) == forum_id:
            return
        previous_message = bot.on_message
        previous_update = getattr(bot, "on_raw_thread_update", None)
        previous_interaction = getattr(bot, "on_interaction", None)
        setattr(bot, INSTALLED, forum_id)

        @bot.event
        async def on_raw_thread_update(payload):
            if previous_update is not None:
                await previous_update(payload)
            data = getattr(payload, "data", None) or {}
            in_forum = str(data.get("parent_id")) == forum_id
            if in_forum and _bridge_alive():
                tags = [str(tag) for tag in data.get("applied_tags") or []]
                _push({"kind": "tags", "thread": str(data.get("id")), "tags": tags})

        @bot.event
        async def on_message(message):
            if await _claim(bot, message, forum_id):
                return
            await previous_message(message)

        @bot.event
        async def on_interaction(interaction):
            if previous_interaction is not None:
                await previous_interaction(interaction)
            await _press(interaction)

        def restore(name, ours, previous):
            if getattr(bot, name, None) is not ours:
                return
            if previous is not None:
                bot.event(previous)
                return
            delattr(bot, name)

        def unload():
            if bot.on_message is on_message:
                bot.event(previous_message)
            restore("on_raw_thread_update", on_raw_thread_update, previous_update)
            restore("on_interaction", on_interaction, previous_interaction)
            if getattr(bot, INSTALLED, None) == forum_id:
                delattr(bot, INSTALLED)

        ctx.on_unload(unload)

    ctx.register_platform_handler("discord", install)

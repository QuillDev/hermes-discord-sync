"""Gateway half: hand the sync forum and @mentions to the backend bridge while it is alive."""

import contextlib
import logging
import time

log = logging.getLogger("hermes.discord-sync")

# The bridge touches the heartbeat every 2 seconds. When the backend is down,
# the normal gateway answers forum posts instead of leaving them unanswered,
# but a sync @mention gets an offline reply, never a native thread.
FRESH_SECONDS = 20
INSTALLED = "_hermes_discord_sync_forum"
SEEN = "👀"
BUTTON_PREFIX = "hds:"
OFFLINE = "discord sync is offline right now. try again in a moment, or answer in the desktop app."


def _sync_dir(profile="default"):
    from hermes_constants import get_hermes_home

    if profile == "default":
        return get_hermes_home() / "discord-sync"
    from hermes_cli.profiles import get_profile_dir

    return get_profile_dir(profile) / "discord-sync"


def _forums(ctx):
    """Forum id -> the profile it syncs: forum_id for default, plus one per entry under profiles."""
    found = {}
    default = str(ctx.get_config("forum_id", ""))
    if default.isdecimal():
        found[default] = "default"
    for name, profile in (ctx.get_config("profiles", None) or {}).items():
        forum = str((profile or {}).get("forum_id", ""))
        if forum.isdecimal() and forum not in found:
            found[forum] = str(name)
    return found


def _routed_profile(message):
    """The profile the gateway's profile_routes send this message to, or "default"."""
    try:
        from gateway.profile_routing import match_profile_route, parse_profile_routes
        from hermes_cli.config import load_config

        routes = parse_profile_routes((load_config().get("gateway") or {}).get("profile_routes"))
    except Exception:
        log.debug("Discord sync could not read profile routes", exc_info=True)
        return "default"
    channel = message.channel
    parent = getattr(channel, "parent_id", None)
    route = match_profile_route(
        routes,
        "discord",
        guild_id=str(getattr(message.guild, "id", "")) or None,
        chat_id=str(channel.id),
        parent_chat_id=str(parent) if parent else None,
        user_id=str(message.author.id),
    )
    return route.profile if route else "default"


def _heartbeat_age(profile="default"):
    """Seconds since the profile's bridge last beat, or None when it never has."""
    try:
        changed = (_sync_dir(profile) / "heartbeat").stat().st_mtime
    except OSError:
        return None
    return time.time() - changed


def _bridge_alive(profile="default"):
    age = _heartbeat_age(profile)
    return age is not None and age < FRESH_SECONDS


def _push(event, profile="default"):
    from .dashboard.sync.inbox import Inbox

    Inbox(_sync_dir(profile) / "inbox").put(event)


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


async def _refuse_mention(message, profile):
    """A sync mention never falls back to a native thread: say the sync is down instead."""
    age = _heartbeat_age(profile)
    shown = "never" if age is None else f"{age:.0f}s ago"
    log.warning("Discord sync refused mention %s: bridge heartbeat %s", message.id, shown)
    with contextlib.suppress(Exception):
        await message.reply(f"{OFFLINE} (last heartbeat: {shown})", mention_author=False)


async def _claim(bot, message, forums):
    """Push a message to the bridge that owns it. False leaves it to the normal bot."""
    channel = message.channel
    profile = forums.get(str(getattr(channel, "parent_id", "")))
    if profile is not None:
        if not _bridge_alive(profile):
            return False
        thread = {"thread": str(channel.id), "name": getattr(channel, "name", None)}
        _push({"kind": "forum", **thread, "message": _message_dict(message)}, profile)
        if _from_allowed_person(message):
            await _seen(message)
        return True
    if not _is_sync_mention(bot, message):
        return False
    # A mention starts a post in the forum of the profile its channel is routed to.
    profile = _routed_profile(message)
    if profile not in forums.values():
        return False
    if not _bridge_alive(profile):
        await _refuse_mention(message, profile)
        return True
    _push({"kind": "mention", "channel": str(channel.id), "message": _message_dict(message)}, profile)
    await _seen(message)
    return True


async def _press(interaction, forums):
    """Acknowledge one of our buttons within Discord's 3 seconds, then hand it to the bridge."""
    data = getattr(interaction, "data", None) or {}
    custom_id = str(data.get("custom_id", ""))
    if not custom_id.startswith(BUTTON_PREFIX):
        return
    user = getattr(interaction, "user", None)
    # Buttons live in a forum post; its forum says which profile's bridge holds the request.
    parent = getattr(getattr(interaction, "channel", None), "parent_id", None)
    profile = forums.get(str(parent), "default")
    if not _bridge_alive(profile):
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
        },
        profile,
    )
    try:
        await interaction.response.defer()
    except Exception:
        log.debug("Discord sync could not acknowledge a button press", exc_info=True)


def register(ctx):
    forums = _forums(ctx)
    if not forums:
        return
    forum_id = ",".join(sorted(forums))

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
            profile = forums.get(str(data.get("parent_id")))
            if profile is not None and _bridge_alive(profile):
                tags = [str(tag) for tag in data.get("applied_tags") or []]
                _push({"kind": "tags", "thread": str(data.get("id")), "tags": tags}, profile)

        @bot.event
        async def on_message(message):
            if await _claim(bot, message, forums):
                return
            await previous_message(message)

        @bot.event
        async def on_interaction(interaction):
            if previous_interaction is not None:
                await previous_interaction(interaction)
            await _press(interaction, forums)

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

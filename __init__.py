"""Hand the sync forum to the desktop bridge while it is alive; leave the rest alone."""
import time

# The bridge touches this every second. If the desktop backend is down, the
# normal gateway answers forum posts as usual instead of leaving them unanswered.
FRESH_SECONDS = 20


def _bridge_alive():
    from hermes_constants import get_hermes_home
    try:
        beat = get_hermes_home() / "discord-sync" / "heartbeat"
        return time.time() - beat.stat().st_mtime < FRESH_SECONDS
    except OSError:
        return False


def _queue_tag_change(data):
    """Hand a forum post's tag change to the bridge; it maps it to a desktop pin."""
    import json

    from hermes_constants import get_hermes_home
    queue = get_hermes_home() / "discord-sync" / "tag-events"
    with queue.open("a") as handle:
        handle.write(json.dumps({"thread": str(data.get("id")),
                                 "tags": [str(tag) for tag in data.get("applied_tags") or []]}) + "\n")


def _queue(name, event):
    import json

    from hermes_constants import get_hermes_home
    with (get_hermes_home() / "discord-sync" / name).open("a") as handle:
        handle.write(json.dumps(event) + "\n")


def _allowed_users():
    from hermes_constants import get_hermes_home
    raw = ""
    try:
        for line in (get_hermes_home() / ".env").read_text().splitlines():
            if line.strip().startswith("DISCORD_ALLOWED_USERS="):
                raw = line.split("=", 1)[1].strip().strip("\"'")
    except OSError:
        pass
    return {user.strip() for user in raw.split(",") if user.strip().isdecimal()}


def _message_dict(message):
    author = message.author
    return {
        "id": str(message.id), "content": message.content or "",
        "author": {"id": str(author.id), "bot": bool(getattr(author, "bot", False))},
        "webhook_id": str(message.webhook_id) if getattr(message, "webhook_id", None) else None,
        "attachments": [{"url": a.url, "size": a.size, "filename": a.filename, "content_type": a.content_type}
                        for a in getattr(message, "attachments", [])],
    }


def register(ctx):
    forum_id = str(ctx.get_config("forum_id", ""))
    if not forum_id.isdecimal():
        return

    def install(bot, adapter):
        # Native discord.py event registration is the documented plugin seam.
        marker = "_hermes_discord_sync_forum"
        if getattr(bot, marker, None) == forum_id:
            return
        previous = bot.on_message
        previous_update = getattr(bot, "on_raw_thread_update", None)
        setattr(bot, marker, forum_id)

        @bot.event
        async def on_raw_thread_update(payload):
            if previous_update is not None:
                await previous_update(payload)
            data = getattr(payload, "data", None) or {}
            if str(data.get("parent_id")) == forum_id and _bridge_alive():
                _queue_tag_change(data)

        @bot.event
        async def on_message(message):
            channel = message.channel
            parent = str(getattr(channel, "parent_id", ""))
            if parent == forum_id and _bridge_alive():
                # Push to the bridge instead of making it poll every thread.
                _queue("inbound", {"kind": "forum", "thread": str(channel.id),
                                   "name": getattr(channel, "name", None), "message": _message_dict(message)})
                return
            mentioned = getattr(bot, "user", None) is not None and bot.user in getattr(message, "mentions", [])
            top_level = getattr(message, "guild", None) is not None and type(channel).__name__ == "TextChannel"
            if (mentioned and top_level and not message.author.bot and _bridge_alive()
                    and str(message.author.id) in _allowed_users()):
                _queue("inbound", {"kind": "mention", "channel": str(channel.id), "message": _message_dict(message)})
                return
            await previous(message)

        def unload():
            if bot.on_message is on_message:
                bot.event(previous)
            if getattr(bot, "on_raw_thread_update", None) is on_raw_thread_update:
                if previous_update is not None:
                    bot.event(previous_update)
                else:
                    delattr(bot, "on_raw_thread_update")
            if getattr(bot, marker, None) == forum_id:
                delattr(bot, marker)

        ctx.on_unload(unload)

    ctx.register_platform_handler("discord", install)

# hermes-discord-sync

A [Hermes Agent](https://hermes-agent.nousresearch.com) plugin that keeps Hermes Desktop sessions and a Discord forum in live two-way sync. Start or continue a conversation from either side and it is the same session: one history, one running turn, replies streamed to both.

No fork. It uses the documented plugin seams only: a dashboard plugin that talks to the backend over the normal session RPC, plus a Discord platform handler in the gateway.

## What it does

- Each synced Desktop session gets one post in a Discord forum channel.
- A new forum post from an allowed user becomes a new Desktop session and is answered there.
- Messages from either side land in the same session. Replies stream into the post while they are generated, with a typing indicator.
- An @mention of the bot in a text channel (from an allowed user) starts a synced forum post and replies with a link to it.
- New Desktop sessions are linked automatically (`auto_link`). Desktop's command palette also has "sync current session".
- Desktop pins map to a `pinned` forum tag, both ways (Discord allows only one pinned post per forum).
- Approvals and clarify questions show up in the post: answer with `!approve <id> <choice>` or `!answer <id> <n> <answer>`. `!stop` interrupts the turn, `!unlink` stops syncing that post.
- Secrets and Desktop-only prompts are never sent to Discord.

While the sync is running, the normal Hermes Discord bot stays out of the forum so nothing is answered twice. If the sync stops, the gateway takes the forum back within about 20 seconds.

## Requirements

- Hermes Agent with the Discord gateway set up (`DISCORD_BOT_TOKEN` and `DISCORD_ALLOWED_USERS` in `~/.hermes/.env`).
- A forum channel in your server. The bot needs to view it, send messages, create posts and manage threads (for tags).
- The Hermes backend running (`hermes serve`, or the one Desktop starts). The sync runs inside it.

## Install

```sh
hermes plugins install https://github.com/QuillDev/hermes-discord-sync
```

Then in `~/.hermes/config.yaml`:

```yaml
plugins:
  enabled:
    - discord-sync
  entries:
    discord-sync:
      settings:
        forum_id: 123456789012345678   # your forum channel id
        auto_link: true                # link new Desktop sessions automatically
```

Restart the gateway and the backend so both halves load.

## How it works

- `dashboard/plugin_api.py` starts a bridge inside the backend. It connects to the backend's session RPC like Desktop does, so Discord input goes through the same `prompt.submit` and turn lock.
- `__init__.py` registers a Discord platform handler in the gateway. It forwards forum messages, @mentions and tag edits to the bridge, and steps aside for the forum while the bridge's heartbeat is fresh.
- `dashboard/sync/store.py` keeps links and the Discord message ids for each session row in `~/.hermes/discord-sync/state.db`, so restarts don't duplicate or replay messages.

## Tests

```sh
pip install -e '.[test]'
pytest
REAL_HERMES=1 pytest tests/test_real_hermes.py   # runs against an installed Hermes, with fake Discord and model
```

## License

MIT

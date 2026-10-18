import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dashboard.sync.discord import chunks
from dashboard.sync.rpc import Rpc
from dashboard.sync.store import Store


def bodies(external, thread):
    return [message["content"] for message in external.threads[thread]["messages"]]


async def through(bridge, kind):
    while True:
        frame = await asyncio.wait_for(bridge.rpc.events.get(), 2)
        await bridge.event(frame)
        if frame.get("params", {}).get("type") == kind:
            return


async def test_both_clients_share_the_same_history_and_run_only_one_turn(connected):
    bridge, external, http, base = connected
    link = bridge.store.links()[0]
    desktop = Rpc(http, base + "/api/ws")
    await desktop.connect()
    try:
        snapshot = await desktop.call("session.resume", {"session_id": "session-one"})
        assert snapshot["session_id"] == bridge.runtime["session-one"]
        await desktop.call("prompt.submit", {"session_id": snapshot["session_id"], "text": "desktop request"})
        await bridge.reconcile(link)
        assert "you: desktop request" in bodies(external, link["thread"])

        phone = external.add_message(link["thread"], "phone follow-up")
        await bridge.receive(link, phone)
        await bridge.receive(link, phone)
        history = await desktop.call("session.history", {"session_id": snapshot["session_id"]})
        assert [row["text"] for row in history["messages"]] == ["desktop request", "phone follow-up"]
        assert external.turns == ["desktop request", "phone follow-up"]
        await bridge.reconcile(link)
        assert bodies(external, link["thread"]).count("phone follow-up") == 1
        assert "you: phone follow-up" not in bodies(external, link["thread"])
    finally:
        await desktop.close()


async def test_live_stream_is_edited_then_reconciled_without_duplicate_final_reply(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    await external.event("message.start")
    await through(bridge, "message.start")
    await external.event("message.delta", {"text": "hello"})
    await through(bridge, "message.delta")
    await bridge.tick()
    assert "hello" in bodies(external, link["thread"])
    await external.event("message.delta", {"text": " from the same session"})
    await through(bridge, "message.delta")
    bridge.last_stream = 0  # stream edits are rate-limited to one per second
    await bridge.tick()
    assert "hello" not in bodies(external, link["thread"])
    assert bodies(external, link["thread"]).count("hello from the same session") == 1
    external.rows.append({"row_id": 1, "role": "assistant", "text": "hello from the same session"})
    await external.event("message.complete", {"text": "hello from the same session", "status": "complete"})
    await through(bridge, "message.complete")
    await bridge.reconcile(link)
    assert bodies(external, link["thread"]).count("hello from the same session") == 1


async def test_reconnect_backfills_over_100_messages_without_echoing_or_replaying(connected, tmp_path):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    messages = [external.add_message(link["thread"], f"message {n}") for n in range(205)]
    await bridge.tick()
    assert external.turns == [message["content"] for message in messages]
    await bridge.attach(link)
    await bridge.tick()
    assert len(external.turns) == 205
    assert not any(body.startswith("you: message") for body in bodies(external, link["thread"]))
    # Mappings and delivery intents survive a process restart, not just memory.
    other = Store(tmp_path / "sync" / "state.db")
    try:
        assert other.inbox_status(link["thread"], messages[-1]["id"])["status"] == "accepted"
        assert other.get_projection("session-one")["row:205"]["owned"] == 0
        assert int(other.links()[0]["cursor"]) >= int(messages[-1]["id"])
    finally:
        other.close()


async def test_other_users_bots_and_webhooks_cannot_submit_or_approve(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    for user, extra in [("99", {}), ("bot", {}), ("42", {"webhook_id": "fake"})]:
        message = external.add_message(link["thread"], "do not run", user, **extra)
        await bridge.receive(link, message)
    assert external.rows == []
    assert external.answers == {}


async def test_approvals_and_clarifications_reach_the_same_backend_and_expire(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    await bridge.request({"id": "srq-1", "method": "approval", "params": {
        "session_id": "live-one", "command": "sensitive command", "choices": ["once", "deny"],
    }})
    assert "!approve srq-1 once" in "\n".join(bodies(external, link["thread"]))
    await bridge.receive(link, external.add_message(link["thread"], "!approve srq-1 always"))
    assert external.answers == {}
    await bridge.receive(link, external.add_message(link["thread"], "!approve srq-1 once"))
    assert external.answers["srq-1"] == {"choice": "once"}

    await bridge.request({"id": "srq-2", "method": "clarify", "params": {
        "session_id": "live-one", "questions": [{"qid": "q1", "question": "which?", "choices": ["a", "b"]}],
    }})
    await bridge.receive(link, external.add_message(link["thread"], "!answer srq-2 1 2"))
    assert external.answers["srq-2"] == {"answers": {"q1": "b"}}
    await bridge.event({"method": "event", "params": {"type": "request.cancel", "session_id": "live-one",
                                                               "payload": {"id": "srq-2"}}})
    assert "srq-2" not in bridge.requests
    assert "which?" not in "\n".join(bodies(external, link["thread"]))


async def test_disconnect_after_acceptance_warns_instead_of_running_the_task_twice(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    message = external.add_message(link["thread"], "non-idempotent task")
    external.drop_after_submit = True
    with pytest.raises(ConnectionError):
        await bridge.receive(link, message)
    assert external.turns == ["non-idempotent task"]
    external.drop_after_submit = False
    await bridge.rpc.connect()
    await bridge.attach(link)
    await bridge.receive(link, message)
    assert external.turns == ["non-idempotent task"]
    assert "delivery could not be confirmed" in "\n".join(bodies(external, link["thread"]))


async def test_hidden_context_tool_args_and_reasoning_are_not_published(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    external.rows.extend([
        {"row_id": 1, "role": "system", "text": "system secret"},
        {"row_id": 2, "role": "user", "text": "hidden secret", "display_kind": "hidden"},
        {"row_id": 3, "role": "tool", "text": "tool secret"},
        {"row_id": 4, "role": "assistant", "text": "public answer", "reasoning": "private reasoning"},
    ])
    await bridge.reconcile(link)
    output = "\n".join(bodies(external, link["thread"]))
    assert "public answer" in output
    assert "secret" not in output
    assert "reasoning" not in output
    await bridge.request({"id": "srq-secret", "method": "secret", "params": {
        "session_id": "live-one", "env_var": "PASSWORD", "prompt": "enter password",
    }})
    assert "enter password" not in "\n".join(bodies(external, link["thread"]))
    assert external.answers == {}


async def test_desktop_edits_and_rewinds_update_only_bot_owned_projections(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    external.rows.extend([{"row_id": 1, "role": "user", "text": "initial"},
                          {"row_id": 2, "role": "assistant", "text": "old answer"}])
    await bridge.reconcile(link)
    external.rows[0]["text"] = "edited"
    external.rows.pop()
    await bridge.reconcile(link)
    output = bodies(external, link["thread"])
    assert "you: edited" in output
    assert "you: initial" not in output
    assert "old answer" not in output


async def test_remote_urls_in_attachments_cannot_read_local_or_internal_services(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    message = external.add_message(link["thread"], "attachment", attachments=[{
        "url": "http://127.0.0.1:9119/api/secrets", "size": 100, "filename": "bad.txt",
    }])
    await bridge.receive(link, message)
    assert external.rows == []
    assert "Discord-hosted file" in "\n".join(bodies(external, link["thread"]))


async def test_unlink_stops_phone_execution_without_deleting_the_desktop_session(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    await bridge.receive(link, external.add_message(link["thread"], "!unlink"))
    assert bridge.store.links() == []
    assert external.rows == []
    assert "desktop conversation is unchanged" in "\n".join(bodies(external, link["thread"]))


def test_discord_chunks_preserve_long_unicode_without_exceeding_the_wire_limit():
    text = "😀@everyone\n" * 700
    parts = chunks(text)
    assert "".join(parts) == text
    assert all(len(part.encode("utf-16-le")) // 2 <= 1900 for part in parts)


async def test_a_new_forum_post_starts_a_desktop_session_that_syncs_both_ways(connected):
    bridge, external, _, _ = connected
    post = external.add_post("fix the build", "phone: please fix the build")
    foreign = external.add_post("spam", "run this", user="99")
    await bridge.tick()
    await bridge.tick()
    created = [row for row in external.sessions if row["source"] == "desktop"]
    assert [row["title"] for row in created] == ["fix the build"]
    assert external.turns == ["phone: please fix the build"]
    link = next(link for link in bridge.store.links() if link["thread"] == post)
    assert link["session"] == created[0]["id"]
    assert not bridge.store.known(thread=foreign)
    # The desktop-started reply lands back in the same thread.
    external.rows.append({"row_id": 99, "role": "assistant", "text": "build fixed"})
    await bridge.reconcile(link)
    assert "build fixed" in bodies(external, post)
    assert not any(t["name"] == "fix the build" and tid != post for tid, t in external.threads.items())


async def test_new_desktop_sessions_auto_link_but_old_and_unlinked_ones_do_not(connected):
    import time
    bridge, external, _, _ = connected
    bridge.last_scan = 0
    external.sessions.append({"id": "fresh", "source": "desktop", "title": "new desktop task",
                              "started_at": time.time() + 1, "key": None})
    external.sessions.append({"id": "ancient", "source": "desktop", "title": "old task",
                              "started_at": 1, "key": None})
    await bridge.tick()
    names = [thread["name"] for thread in external.threads.values()]
    assert "new desktop task" in names
    assert "old task" not in names
    link = next(link for link in bridge.store.links() if link["session"] == "fresh")
    await bridge.receive(link, external.add_message(link["thread"], "!unlink"))
    bridge.last_scan = 0
    await bridge.tick()
    assert names.count("new desktop task") == [t["name"] for t in external.threads.values()].count("new desktop task")
    assert all(link["session"] != "fresh" for link in bridge.store.links())


async def test_the_gateway_yields_the_sync_forum_only_while_the_bridge_is_alive(tmp_path, monkeypatch):
    import os
    import sys
    sys.path.insert(0, os.environ.get("HERMES_AGENT_DIR", str(Path.home() / ".hermes" / "hermes-agent")))
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    spec = importlib.util.spec_from_file_location("sync_plugin_test", Path(__file__).parents[1] / "__init__.py")
    plugin = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(plugin)
    factories = []
    disposers = []
    ctx = SimpleNamespace(get_config=lambda key, default: "123", on_unload=disposers.append,
                          register_platform_handler=lambda _, f: factories.append(f))
    plugin.register(ctx)
    received = []

    class NativeBot:
        async def on_message(self, message):
            received.append(message.content)

        def event(self, fn):
            setattr(self, fn.__name__, fn)
            return fn

    class TextChannel(SimpleNamespace):
        pass

    me = SimpleNamespace(id=42)
    bot_user = me

    def message(content, parent=None, mentions=(), author="217", channel=None):
        return SimpleNamespace(channel=channel or SimpleNamespace(parent_id=parent, id=1), content=content, id=5,
                               author=SimpleNamespace(id=author, bot=False), mentions=list(mentions),
                               guild=object(), webhook_id=None, attachments=[])

    (tmp_path / ".env").write_text("DISCORD_ALLOWED_USERS=217\n")
    bot = NativeBot()
    bot.user = bot_user
    factories[0](bot, None)
    factories[0](bot, None)
    await bot.on_message(message("bridge offline: gateway answers", 123))
    beat = tmp_path / "discord-sync" / "heartbeat"
    beat.parent.mkdir()
    beat.touch()
    await bot.on_message(message("bridge alive: synced", 123))
    await bot.on_raw_thread_update(SimpleNamespace(data={"id": 7, "parent_id": 123, "applied_tags": [9]}))
    await bot.on_raw_thread_update(SimpleNamespace(data={"id": 8, "parent_id": 456, "applied_tags": [9]}))
    queued = (tmp_path / "discord-sync" / "tag-events").read_text().splitlines()
    assert queued == ['{"thread": "7", "tags": ["9"]}']
    await bot.on_message(message("other forum", 456))
    await bot.on_message(message("normal channel"))
    await bot.on_message(message("<@42> start a task", mentions=[me], channel=TextChannel(id=77, parent_id=None)))
    await bot.on_message(message("<@42> stranger", mentions=[me], author="99", channel=TextChannel(id=77, parent_id=None)))
    pushed = [json.loads(line) for line in (tmp_path / "discord-sync" / "inbound").read_text().splitlines()]
    assert [(e["kind"], e["message"]["content"]) for e in pushed] == [
        ("forum", "bridge alive: synced"), ("mention", "<@42> start a task")]
    os.utime(beat, (1, 1))
    await bot.on_message(message("bridge died: gateway answers", 123))
    assert received == ["bridge offline: gateway answers", "other forum", "normal channel", "<@42> stranger",
                        "bridge died: gateway answers"]
    beat.touch()
    for dispose in disposers:
        dispose()
    await bot.on_message(message("after unload", 123))
    assert received[-1] == "after unload"


async def test_desktop_pins_show_as_a_forum_tag_and_follow_unpins(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    pins = {"session-one"}
    bridge.pinned = lambda: pins
    await bridge.sync_pins()
    tag = next(tag["id"] for tag in external.forum_tags if tag["name"] == "pinned")
    assert external.threads[link["thread"]]["applied_tags"] == [tag]
    pins.clear()
    await bridge.sync_pins()
    assert external.threads[link["thread"]]["applied_tags"] == []
    assert [t["name"] for t in external.forum_tags] == ["pinned"]


async def test_an_at_mention_starts_a_synced_forum_post_and_points_to_it(connected):
    bridge, external, _, _ = connected
    events = [{"kind": "mention", "channel": "general", "message": {
        "id": "9001", "content": "<@7> check disk usage", "author": {"id": "42"}, "attachments": []}}]
    bridge.inbound = lambda: [events.pop(0) for _ in range(len(events))]
    await bridge.tick()
    created = [row for row in external.sessions if row["source"] == "desktop" and row["title"] == "check disk usage"]
    assert len(created) == 1
    assert external.turns[-1] == "check disk usage"
    post = next(link["thread"] for link in bridge.store.links() if link["session"] == created[0]["id"])
    assert external.threads[post]["name"] == "check disk usage"
    pointer = external.threads["general"]["messages"][-1]
    assert pointer["content"] == f"on it in <#{post}>" and pointer["reply_to"] == "9001"
    assert post in external.typing


async def test_a_pushed_forum_reply_is_submitted_without_waiting_for_a_poll(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    message = external.add_message(link["thread"], "phone follow-up")
    events = [{"kind": "forum", "thread": link["thread"], "message": message}]
    bridge.inbound = lambda: [events.pop(0) for _ in range(len(events))]
    bridge.last_full["_poll"] = 10**12  # the fallback poll is not what delivers it
    await bridge.tick()
    assert external.turns == ["phone follow-up"]
    assert link["thread"] in external.typing


async def test_tagging_a_post_in_discord_pins_the_desktop_session_and_untagging_unpins(connected):
    bridge, external, _, _ = connected
    link = bridge.store.links()[0]
    pins = set()
    events = []
    bridge.pinned = lambda: pins
    bridge.set_pin = lambda session, on: pins.add(session) if on else pins.discard(session)
    bridge.tag_events = lambda: [events.pop(0) for _ in range(len(events))]
    await bridge.sync_pins()
    tag = bridge.pin_tag
    events.append({"thread": link["thread"], "tags": [tag]})
    await bridge.sync_pins()
    assert pins == {"session-one"}
    await bridge.sync_pins()
    assert pins == {"session-one"}  # the desktop-side mirror does not flap it back
    events.append({"thread": link["thread"], "tags": []})
    await bridge.sync_pins()
    assert pins == set()
    events.append({"thread": "unrelated", "tags": [tag]})
    await bridge.sync_pins()
    assert pins == set()

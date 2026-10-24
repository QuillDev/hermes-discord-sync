import asyncio
import importlib.util
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import pytest

from dashboard.sync.bridge import AUDIT_LAG, POLL_EVERY, SCAN_EVERY, STREAM_EDIT_EVERY, Bridge
from dashboard.sync.discord import Discord, chunks
from dashboard.sync.inbox import Inbox
from dashboard.sync.rpc import Rpc
from dashboard.sync.store import Store
from dashboard.sync.wording import Wording

HERMES_AGENT = Path(os.environ.get("HERMES_AGENT_DIR", Path.home() / ".hermes" / "hermes-agent"))
PLUGIN_ROOT = Path(__file__).parents[1]


def bodies(external, thread):
    return [message["content"] for message in external.threads[thread]["messages"]]


def thread_text(external, thread):
    return "\n".join(bodies(external, thread))


async def through(bridge, kind):
    """Deliver backend events to the bridge until one of the given type arrives."""
    while True:
        frame = await asyncio.wait_for(bridge.rpc.events.get(), 2)
        await bridge.event(frame)
        if frame.get("params", {}).get("type") == kind:
            return


async def test_both_clients_share_the_same_history_and_run_only_one_turn(world):
    bridge, external, link = world.bridge, world.external, world.link
    desktop = Rpc(world.http, world.base + "/api/ws")
    await desktop.connect()
    try:
        snapshot = await desktop.call("session.resume", {"session_id": "session-one"})
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


async def test_isolated_turn_from_discord_is_not_echoed_but_desktop_input_is(world):
    bridge, external, link = world.bridge, world.external, world.link
    external.isolated = True
    phone = external.add_message(link["thread"], "phone question")
    await bridge.receive(link, phone)
    await bridge.reconcile(link)
    await bridge.reconcile(link)
    assert not any(body.startswith("you: phone question") for body in bodies(external, link["thread"]))

    desktop = Rpc(world.http, world.base + "/api/ws")
    await desktop.connect()
    try:
        await desktop.call("prompt.submit", {"session_id": "live-one", "text": "desktop question"})
    finally:
        await desktop.close()
    await bridge.reconcile(link)
    assert any(body.startswith("you: desktop question") for body in bodies(external, link["thread"]))


async def test_live_stream_is_edited_then_reconciled_without_duplicate_final_reply(world):
    bridge, external, link = world.bridge, world.external, world.link
    await external.event("message.start")
    await through(bridge, "message.start")
    await external.event("message.delta", {"text": "hello"})
    await through(bridge, "message.delta")
    await bridge.tick()
    assert "hello" in bodies(external, link["thread"])
    await external.event("message.delta", {"text": " from the same session"})
    await through(bridge, "message.delta")
    world.clock.advance(STREAM_EDIT_EVERY)
    await bridge.tick()
    assert "hello" not in bodies(external, link["thread"])
    assert bodies(external, link["thread"]).count("hello from the same session") == 1
    external.rows.append({"row_id": 1, "role": "assistant", "text": "hello from the same session"})
    await external.event("message.complete", {"text": "hello from the same session", "status": "complete"})
    await through(bridge, "message.complete")
    assert bodies(external, link["thread"]).count("hello from the same session") == 1


async def test_reconnect_backfills_over_100_messages_without_echoing_or_replaying(world, tmp_path):
    bridge, external, link = world.bridge, world.external, world.link
    messages = [external.add_message(link["thread"], f"message {n}") for n in range(205)]
    await bridge.tick()
    assert external.turns == [message["content"] for message in messages]
    await bridge.attach(link)
    world.clock.advance(POLL_EVERY)
    await bridge.tick()
    assert len(external.turns) == 205
    assert not any(body.startswith("you: message") for body in bodies(external, link["thread"]))
    # Mappings and delivery intents survive a process restart, not just memory.
    reopened = Store(tmp_path / "sync" / "state.db")
    try:
        assert reopened.inbox_status(link["thread"], messages[-1]["id"])["status"] == "accepted"
        assert reopened.get_projection("session-one")["row:205"]["owned"] == 0
        assert int(reopened.links()[0]["cursor"]) >= int(messages[-1]["id"])
    finally:
        reopened.close()


async def test_other_users_bots_and_webhooks_cannot_submit_or_approve(world):
    bridge, external, link = world.bridge, world.external, world.link
    for user, extra in [("99", {}), ("bot", {}), ("42", {"webhook_id": "fake"})]:
        message = external.add_message(link["thread"], "do not run", user, **extra)
        await bridge.receive(link, message)
    assert external.rows == []
    assert external.answers == {}


def buttons(message):
    return [button for row in message.get("components", []) for button in row["components"]]


def card(external, thread, text):
    return next(m for m in external.threads[thread]["messages"] if text in m["content"])


async def test_approvals_are_buttons_that_resolve_in_place(world):
    bridge, external, link = world.bridge, world.external, world.link
    approval = {"session_id": "live-one", "command": "rm -rf build", "choices": ["once", "session", "deny"]}
    await bridge.show_request({"id": "srq-1", "method": "approval", "params": approval})
    prompt = card(external, link["thread"], "rm -rf build")
    assert [button["label"] for button in buttons(prompt)] == ["Allow Once", "Allow Session", "Deny"]
    # A stranger's press does nothing.
    world.inbox.put(
        {"kind": "press", "custom_id": buttons(prompt)[0]["custom_id"], "user": "99", "name": "x"}
    )
    await bridge.tick()
    assert external.answers == {}
    world.inbox.put(
        {"kind": "press", "custom_id": buttons(prompt)[0]["custom_id"], "user": "42", "name": "Quill"}
    )
    await bridge.tick()
    assert external.answers["srq-1"] == {"choice": "once"}
    # The card stays, greyed out, saying who decided what.
    assert "Approved once by Quill" in prompt["content"]
    assert all(button["disabled"] for button in buttons(prompt))


async def test_a_plain_yes_answers_an_open_approval_and_the_text_command_still_works(world):
    bridge, external, link = world.bridge, world.external, world.link
    approval = {"session_id": "live-one", "command": "deploy", "choices": ["once", "deny"]}
    await bridge.show_request({"id": "srq-1", "method": "approval", "params": approval})
    await bridge.receive(link, external.add_message(link["thread"], "yes"))
    assert external.answers["srq-1"] == {"choice": "once"}
    assert external.turns == []
    await bridge.show_request({"id": "srq-2", "method": "approval", "params": approval})
    await bridge.receive(link, external.add_message(link["thread"], "!approve srq-2 always"))
    assert "srq-2" not in external.answers
    await bridge.receive(link, external.add_message(link["thread"], "!approve srq-2 deny"))
    assert external.answers["srq-2"] == {"choice": "deny"}


async def test_clarify_options_are_buttons_and_a_typed_reply_answers_the_next_question(world):
    bridge, external, link = world.bridge, world.external, world.link
    questions = [
        {"qid": "q1", "question": "which branch?", "choices": ["main", "develop"]},
        {"qid": "q2", "question": "why?"},
    ]
    external.questions["srq-2"] = ["q1", "q2"]
    clarify = {"session_id": "live-one", "questions": questions}
    await bridge.show_request({"id": "srq-2", "method": "clarify", "params": clarify})
    prompt = card(external, link["thread"], "which branch?")
    assert [button["label"] for button in buttons(prompt)] == ["1. main", "2. develop"]
    world.inbox.put({"kind": "press", "custom_id": buttons(prompt)[1]["custom_id"], "user": "42"})
    await bridge.tick()
    assert "→ develop" in prompt["content"]
    assert external.answers == {}
    await bridge.receive(link, external.add_message(link["thread"], "release cut"))
    assert external.answers["srq-2"] == {"answers": {"q1": "develop", "q2": "release cut"}}
    # The reply answered the question; it never became a prompt.
    assert external.turns == []


async def test_a_reply_after_the_question_was_answered_elsewhere_reaches_the_agent(world):
    bridge, external, link = world.bridge, world.external, world.link
    clarify = {"session_id": "live-one", "questions": [{"qid": "q1", "question": "which?", "choices": ["a"]}]}
    await bridge.show_request({"id": "srq-4", "method": "clarify", "params": clarify})
    # Desktop answered it; the host sends no cancel for that.
    external.settled.add("srq-4")
    await bridge.receive(link, external.add_message(link["thread"], "are you there?"))
    assert external.turns == ["are you there?"]
    prompt = card(external, link["thread"], "which?")
    assert "closed" in prompt["content"]
    assert "are you there?" not in prompt["content"]


async def test_a_cancelled_request_disappears_and_its_command_reports_it_closed(world):
    bridge, external, link = world.bridge, world.external, world.link
    clarify = {"session_id": "live-one", "questions": [{"qid": "q1", "question": "which?", "choices": ["a"]}]}
    await bridge.show_request({"id": "srq-3", "method": "clarify", "params": clarify})
    cancel = {"type": "request.cancel", "session_id": "live-one", "payload": {"id": "srq-3"}}
    await bridge.event({"method": "event", "params": cancel})
    assert "which?" not in thread_text(external, link["thread"])
    await bridge.receive(link, external.add_message(link["thread"], "!answer srq-3 1 1"))
    assert "srq-3" not in external.answers
    assert "no longer open" in thread_text(external, link["thread"])


async def test_message_status_shows_as_reactions_like_the_native_bot(world):
    bridge, external, link = world.bridge, world.external, world.link
    thread = link["thread"]
    first = external.add_message(thread, "build it")
    await bridge.receive(link, first)
    assert external.reacted(thread, first["id"]) == ["👀"]
    # While that turn runs, a follow-up is steered in and says so.
    external.busy_status = "steered"
    steer = external.add_message(thread, "use the fast path")
    await bridge.receive(link, steer)
    assert external.reacted(thread, steer["id"]) == ["⏩"]
    ack = external.threads[thread]["messages"][-1]
    assert ack["reply_to"] == steer["id"]
    assert ack["content"].startswith("⏩ Steered into current run")
    # A queued one waits with ⏳ and takes over 👀 when its turn starts.
    external.busy_status = "queued"
    queued = external.add_message(thread, "then deploy")
    await bridge.receive(link, queued)
    assert external.reacted(thread, queued["id"]) == ["⏳"]
    external.rows.append({"row_id": 50, "role": "assistant", "text": "built"})
    await external.event("message.complete", {"status": "complete", "text": "built"})
    await through(bridge, "message.complete")
    assert external.reacted(thread, first["id"]) == ["✅"]
    assert external.reacted(thread, queued["id"]) == ["👀"]
    await external.event("message.complete", {"status": "interrupted", "text": ""})
    await through(bridge, "message.complete")
    assert external.reacted(thread, queued["id"]) == ["⏹️"]


class QuietWording(Wording):
    def __init__(self, **settings):
        self.settings = settings

    def display(self, setting, default):
        return self.settings.get(setting, default)


async def test_busy_acks_follow_the_display_settings_but_reactions_stay(world):
    bridge, external, link = world.bridge, world.external, world.link
    thread = link["thread"]
    await bridge.receive(link, external.add_message(thread, "build it"))
    bridge.words = QuietWording(busy_steer_ack_enabled=False)
    external.busy_status = "steered"
    steer = external.add_message(thread, "use the fast path")
    await bridge.receive(link, steer)
    assert external.reacted(thread, steer["id"]) == ["⏩"]
    assert not any(m.get("reply_to") == steer["id"] for m in external.threads[thread]["messages"])
    # Only the steer ack is off: a queued message still says so.
    external.busy_status = "queued"
    queued = external.add_message(thread, "then deploy")
    await bridge.receive(link, queued)
    assert external.threads[thread]["messages"][-1]["content"].startswith("⏳ Queued")
    # busy_ack_enabled off hides them all.
    bridge.words = QuietWording(busy_ack_enabled="false")
    later = external.add_message(thread, "and test it")
    await bridge.receive(link, later)
    assert external.reacted(thread, later["id"]) == ["⏳"]
    assert not any(m.get("reply_to") == later["id"] for m in external.threads[thread]["messages"])


async def test_a_reply_to_a_redirect_lands_below_the_redirecting_message(world):
    bridge, external, link = world.bridge, world.external, world.link
    thread = link["thread"]
    await external.event("message.start")
    await through(bridge, "message.start")
    await external.event("tool.start", {"name": "web_search", "preview": "contabo plans"})
    await through(bridge, "tool.start")
    await bridge.tick()
    assert any("web_search" in body for body in bodies(external, thread))
    external.busy_status = "redirected"
    redirect = external.add_message(thread, "are contabo vds shared?")
    await bridge.receive(link, redirect)
    await external.event("message.delta", {"text": "no, mostly not."})
    await through(bridge, "message.delta")
    world.clock.advance(STREAM_EDIT_EVERY)
    await bridge.tick()
    external.rows.append({"row_id": 7, "role": "assistant", "text": "no, mostly not."})
    await external.event("message.complete", {"status": "complete", "text": "no, mostly not."})
    await through(bridge, "message.complete")
    shown = bodies(external, thread)
    assert shown.count("no, mostly not.") == 1
    assert shown.index("no, mostly not.") > shown.index("are contabo vds shared?")


async def test_a_rejected_message_gets_a_failure_reaction(world):
    bridge, external, link = world.bridge, world.external, world.link
    attachment = {"url": "http://127.0.0.1:9119/x", "size": 1, "filename": "x"}
    message = external.add_message(link["thread"], "file", attachments=[attachment])
    await bridge.receive(link, message)
    assert external.reacted(link["thread"], message["id"]) == ["❌"]


async def test_the_live_message_shows_tools_then_becomes_the_reply_in_place(world):
    bridge, external, link = world.bridge, world.external, world.link
    thread = link["thread"]
    await external.event("message.start")
    await through(bridge, "message.start")
    await external.event("tool.start", {"name": "terminal", "preview": "git status"})
    await through(bridge, "tool.start")
    await bridge.tick()
    live = external.threads[thread]["messages"][-1]
    assert "terminal: git status" in live["content"]
    await external.event("message.delta", {"text": "all clean"})
    await through(bridge, "message.delta")
    world.clock.advance(STREAM_EDIT_EVERY)
    await bridge.tick()
    assert live["content"].endswith("all clean")
    external.rows.append({"row_id": 7, "role": "assistant", "text": "all clean."})
    await external.event("message.complete", {"status": "complete", "text": "all clean."})
    await through(bridge, "message.complete")
    # Same message, now the finished reply: nothing deleted and reposted.
    assert live in external.threads[thread]["messages"]
    assert live["content"] == "all clean."
    assert bodies(external, thread).count("all clean.") == 1


async def test_each_turn_gets_its_own_live_message(world):
    bridge, external, link = world.bridge, world.external, world.link
    for _ in range(2):
        await external.event("message.start")
        await through(bridge, "message.start")
        await external.event("message.delta", {"text": "same words"})
        await through(bridge, "message.delta")
        world.clock.advance(STREAM_EDIT_EVERY)
        await bridge.tick()
        # Discord deduplicates by nonce: a reused one hands back the first turn's deleted message.
        assert bodies(external, link["thread"]).count("same words") == 1
        await external.event("message.complete", {"status": "error", "error": "boom"})
        await through(bridge, "message.complete")
        assert bodies(external, link["thread"]).count("same words") == 0


async def test_malformed_commands_show_usage_and_reach_nothing(world):
    bridge, external, link = world.bridge, world.external, world.link
    await bridge.receive(link, external.add_message(link["thread"], "!approve srq-1"))
    await bridge.receive(link, external.add_message(link["thread"], "!answer srq-1 one two"))
    assert external.answers == {}
    assert thread_text(external, link["thread"]).count("commands: !stop") == 2


async def test_disconnect_after_acceptance_warns_instead_of_running_the_task_twice(world):
    bridge, external, link = world.bridge, world.external, world.link
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
    assert "delivery could not be confirmed" in thread_text(external, link["thread"])


async def test_hidden_context_tool_args_and_reasoning_are_not_published(world):
    bridge, external, link = world.bridge, world.external, world.link
    external.rows.extend(
        [
            {"row_id": 1, "role": "system", "text": "system secret"},
            {"row_id": 2, "role": "user", "text": "hidden secret", "display_kind": "hidden"},
            {"row_id": 3, "role": "tool", "text": "tool secret"},
            {"row_id": 4, "role": "assistant", "text": "public answer", "reasoning": "private reasoning"},
            {"row_id": 5, "role": "user", "text": "process secret", "display_kind": "process_complete"},
            {"row_id": 6, "role": "user", "text": "subagent secret", "display_kind": "async_delegation_complete"},
            {"row_id": 7, "role": "user", "text": "steered mid-turn", "display_kind": "steer"},
        ]
    )
    await bridge.reconcile(link)
    output = thread_text(external, link["thread"])
    assert "public answer" in output
    assert "steered mid-turn" in output
    assert "secret" not in output
    assert "reasoning" not in output
    secret = {"session_id": "live-one", "env_var": "PASSWORD", "prompt": "enter password"}
    await bridge.show_request({"id": "srq-secret", "method": "secret", "params": secret})
    assert "enter password" not in thread_text(external, link["thread"])
    assert external.answers == {}


async def test_session_references_link_to_their_discord_threads(world):
    bridge, external, link = world.bridge, world.external, world.link
    bridge.store.link("20261016_131553_76b5dd", "777", "synced task", "0")
    world.native_threads["20261020_004724_b790b548"] = "888"
    external.rows.append(
        {
            "row_id": 1,
            "role": "assistant",
            "text": "see @session:default/20261016_131553_76b5dd and `@session:20261020_004724_b790b548`, "
            "not @session:default/20261019_102550_9024fb",
        }
    )
    await bridge.reconcile(link)
    assert "see <#777> and <#888>, not @session:default/20261019_102550_9024fb" in bodies(
        external, link["thread"]
    )


async def test_desktop_edits_and_rewinds_update_only_bot_owned_projections(world):
    bridge, external, link = world.bridge, world.external, world.link
    external.rows.extend(
        [
            {"row_id": 1, "role": "user", "text": "initial"},
            {"row_id": 2, "role": "assistant", "text": "old answer"},
        ]
    )
    await bridge.reconcile(link)
    external.rows[0]["text"] = "edited"
    external.rows.pop()
    await bridge.reconcile(link)
    output = bodies(external, link["thread"])
    assert "you: edited" in output
    assert "you: initial" not in output
    assert "old answer" not in output


async def test_remote_urls_in_attachments_cannot_read_local_or_internal_services(world):
    bridge, external, link = world.bridge, world.external, world.link
    attachment = {"url": "http://127.0.0.1:9119/api/secrets", "size": 100, "filename": "bad.txt"}
    message = external.add_message(link["thread"], "attachment", attachments=[attachment])
    await bridge.receive(link, message)
    assert external.rows == []
    assert "Discord-hosted file" in thread_text(external, link["thread"])


async def test_unlink_stops_phone_execution_without_deleting_the_desktop_session(world):
    bridge, external, link = world.bridge, world.external, world.link
    await bridge.receive(link, external.add_message(link["thread"], "!unlink"))
    assert bridge.store.links() == []
    assert external.rows == []
    assert "desktop conversation is unchanged" in thread_text(external, link["thread"])


def test_discord_chunks_preserve_long_unicode_without_exceeding_the_wire_limit():
    text = "😀@everyone\n" * 700
    parts = chunks(text)
    assert "".join(parts) == text
    assert all(len(part.encode("utf-16-le")) // 2 <= 1900 for part in parts)


async def test_a_new_forum_post_starts_a_desktop_session_that_syncs_both_ways(world):
    bridge, external = world.bridge, world.external
    post = external.add_post("fix the build", "phone: please fix the build")
    foreign = external.add_post("spam", "run this", user="99")
    await bridge.tick()
    created = [row for row in external.sessions if row["source"] == "desktop"]
    assert [row["title"] for row in created] == ["fix the build"]
    assert external.turns == ["phone: please fix the build"]
    link = next(link for link in bridge.store.links() if link["thread"] == post)
    assert link["session"] == created[0]["id"]
    assert not bridge.store.known(thread=foreign)
    # The desktop-started reply lands back in the same thread, not a new one.
    external.rows.append({"row_id": 99, "role": "assistant", "text": "build fixed"})
    await bridge.reconcile(link)
    assert "build fixed" in bodies(external, post)
    duplicates = [tid for tid, thread in external.threads.items() if thread["name"] == "fix the build"]
    assert duplicates == [post]


async def test_new_desktop_sessions_auto_link_but_old_and_unlinked_ones_do_not(world):
    bridge, external = world.bridge, world.external
    fresh = {"id": "fresh", "source": "desktop", "title": "new desktop task", "started_at": time.time() + 1}
    ancient = {"id": "ancient", "source": "desktop", "title": "old task", "started_at": 1}
    external.sessions.extend([{**fresh, "key": None}, {**ancient, "key": None}])
    await bridge.tick()
    names = [thread["name"] for thread in external.threads.values()]
    assert "new desktop task" in names
    assert "old task" not in names
    link = next(link for link in bridge.store.links() if link["session"] == "fresh")
    await bridge.receive(link, external.add_message(link["thread"], "!unlink"))
    world.clock.advance(SCAN_EVERY)
    await bridge.tick()
    names_after = [thread["name"] for thread in external.threads.values()]
    assert names_after.count("new desktop task") == 1
    assert all(link["session"] != "fresh" for link in bridge.store.links())


async def test_desktop_pins_show_as_a_forum_tag_and_follow_unpins(world):
    bridge, external, link = world.bridge, world.external, world.link
    world.pins.set("session-one", True)
    await bridge.tick()
    tag = next(tag["id"] for tag in external.forum_tags if tag["name"] == "pinned")
    assert external.threads[link["thread"]]["applied_tags"] == [tag]
    world.pins.set("session-one", False)
    await bridge.tick()
    assert external.threads[link["thread"]]["applied_tags"] == []
    assert [tag["name"] for tag in external.forum_tags] == ["pinned"]


async def test_tagging_a_post_in_discord_pins_the_desktop_session_and_untagging_unpins(world):
    bridge, link = world.bridge, world.link
    await bridge.tick()
    tag = next(tag["id"] for tag in world.external.forum_tags if tag["name"] == "pinned")
    world.inbox.put({"kind": "tags", "thread": link["thread"], "tags": [tag]})
    await bridge.tick()
    assert world.pins.pinned() == {"session-one"}
    await bridge.tick()
    # Mirroring the pin back to Discord does not flap it.
    assert world.pins.pinned() == {"session-one"}
    world.inbox.put({"kind": "tags", "thread": link["thread"], "tags": []})
    await bridge.tick()
    assert world.pins.pinned() == set()
    world.inbox.put({"kind": "tags", "thread": "unrelated", "tags": [tag]})
    await bridge.tick()
    assert world.pins.pinned() == set()


async def test_an_at_mention_starts_a_synced_forum_post_and_points_to_it(world):
    bridge, external = world.bridge, world.external
    message = {"id": "9001", "content": "<@7> check disk usage", "author": {"id": "42"}, "attachments": []}
    world.inbox.put({"kind": "mention", "channel": "general", "message": message})
    await bridge.tick()
    created = [row for row in external.sessions if row["title"] == "check disk usage"]
    assert [row["source"] for row in created] == ["desktop"]
    assert external.turns[-1] == "check disk usage"
    post = next(link["thread"] for link in bridge.store.links() if link["session"] == created[0]["id"])
    assert external.threads[post]["name"] == "check disk usage"
    pointer = external.threads["general"]["messages"][-1]
    assert pointer["content"] == f"on it in <#{post}>"
    assert pointer["reply_to"] == "9001"
    assert post in external.typing
    # The same pushed mention again (a gateway retry) starts nothing new.
    world.inbox.put({"kind": "mention", "channel": "general", "message": message})
    await bridge.tick()
    assert len([row for row in external.sessions if row["title"] == "check disk usage"]) == 1


async def test_a_profile_forum_keeps_its_sessions_in_that_profile(world, tmp_path):
    external = world.external
    async with aiohttp.ClientSession() as http:
        rpc = Rpc(http, world.base + "/api/ws")
        await rpc.connect()
        personal = Bridge(
            rpc,
            Discord(http, "test-token", world.base + "/discord"),
            Store(tmp_path / "personal" / "state.db"),
            "personal-forum",
            ["42"],
            clock=world.clock,
            profile="personal",
        )
        await personal.discord.validate("personal-forum")
        # Posts from before a bridge started belong to the native gateway.
        await asyncio.sleep(0.01)
        try:
            post = external.add_post("plan the trip", "book flights", parent="personal-forum")
            work_post = external.add_post("fix the build", "please fix it")
            await personal.tick()
            await world.bridge.tick()
            by_title = {row["title"]: row for row in external.sessions}
            assert by_title["plan the trip"]["profile"] == "personal"
            assert by_title["fix the build"]["profile"] == "default"
            assert [link["thread"] for link in personal.store.links()] == [post]
            assert work_post in [link["thread"] for link in world.bridge.store.links()]
            assert post not in [link["thread"] for link in world.bridge.store.links()]
            # A new personal desktop session gets a post in the personal forum only.
            fresh = {"id": "diary", "source": "desktop", "title": "diary", "started_at": time.time() + 1}
            external.sessions.append({**fresh, "key": None, "profile": "personal"})
            world.clock.advance(SCAN_EVERY)
            await personal.tick()
            await world.bridge.tick()
            diary = [thread for thread in external.threads.values() if thread["name"] == "diary"]
            assert [thread["parent_id"] for thread in diary] == ["personal-forum"]
            with pytest.raises(ValueError):
                await world.bridge.link("diary")
        finally:
            await personal.close()
            personal.store.close()


async def test_a_pushed_forum_reply_runs_once_even_when_the_poll_sees_it_too(world):
    bridge, external, link = world.bridge, world.external, world.link
    await bridge.tick()
    message = {"id": "5000", "content": "phone follow-up", "author": {"id": "42"}}
    world.inbox.put({"kind": "forum", "thread": link["thread"], "message": message})
    # Not in the simulated thread yet, so only the push can deliver it.
    await bridge.tick()
    assert external.turns == ["phone follow-up"]
    assert link["thread"] in external.typing
    external.threads[link["thread"]]["messages"].append({**message, "channel_id": link["thread"]})
    world.clock.advance(POLL_EVERY)
    await bridge.tick()
    assert external.turns == ["phone follow-up"]
    assert world.inbox.pending() == []


@pytest.mark.skipif(not HERMES_AGENT.is_dir(), reason="needs a Hermes Agent checkout (HERMES_AGENT_DIR)")
async def test_the_gateway_yields_the_sync_forum_while_the_bridge_is_alive_and_never_threads_a_mention(
    tmp_path, monkeypatch
):
    sys.path.insert(0, str(HERMES_AGENT))
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("DISCORD_ALLOWED_USERS", "217")
    # Load it the way Hermes does: as a package, so its relative imports resolve.
    spec = importlib.util.spec_from_file_location(
        "sync_plugin_under_test", PLUGIN_ROOT / "__init__.py", submodule_search_locations=[str(PLUGIN_ROOT)]
    )
    plugin = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = plugin
    spec.loader.exec_module(plugin)
    installers = []
    disposers = []
    ctx = SimpleNamespace(
        get_config=lambda key, default: {"forum_id": "123"}.get(key, default),
        on_unload=disposers.append,
        register_platform_handler=lambda platform, install: installers.append(install),
    )
    plugin.register(ctx)
    answered_by_gateway = []

    class NativeBot:
        user = SimpleNamespace(id=42)

        async def on_message(self, message):
            answered_by_gateway.append(message.content)

        def event(self, handler):
            setattr(self, handler.__name__, handler)
            return handler

    class TextChannel(SimpleNamespace):
        pass

    bot = NativeBot()
    seen = []

    replies = []

    def message(content, parent=None, mentions=(), author="217", channel=None):
        async def add_reaction(emoji):
            seen.append((content, emoji))

        async def reply(text, mention_author=True):
            replies.append((content, text))

        return SimpleNamespace(
            channel=channel or SimpleNamespace(parent_id=parent, id=1),
            content=content,
            id=5,
            author=SimpleNamespace(id=author, bot=False),
            mentions=list(mentions),
            guild=object(),
            webhook_id=None,
            attachments=[],
            add_reaction=add_reaction,
            reply=reply,
        )

    acknowledged = []

    def press(custom_id, user="217"):
        async def defer():
            acknowledged.append(("defer", custom_id))

        async def send_message(text, ephemeral=False):
            acknowledged.append(("offline", custom_id))

        response = SimpleNamespace(defer=defer, send_message=send_message)
        user = SimpleNamespace(id=user, display_name="Quill")
        return SimpleNamespace(data={"custom_id": custom_id}, user=user, response=response)

    text_channel = TextChannel(id=77, parent_id=None)
    installers[0](bot, None)
    installers[0](bot, None)
    await bot.on_message(message("bridge offline: gateway answers", 123))
    beat = tmp_path / "discord-sync" / "heartbeat"
    beat.parent.mkdir()
    beat.touch()
    await bot.on_message(message("bridge alive: synced", 123))
    await bot.on_raw_thread_update(SimpleNamespace(data={"id": 7, "parent_id": 123, "applied_tags": [9]}))
    await bot.on_raw_thread_update(SimpleNamespace(data={"id": 8, "parent_id": 456, "applied_tags": [9]}))
    await bot.on_message(message("other forum", 456))
    await bot.on_message(message("normal channel"))
    await bot.on_message(message("<@42> start a task", mentions=[bot.user], channel=text_channel))
    await bot.on_message(message("<@42> stranger", mentions=[bot.user], author="99", channel=text_channel))
    await bot.on_message(message("someone else in the forum", 123, author="99"))
    await bot.on_interaction(press("hds:approve:srq-1:once"))
    await bot.on_interaction(press("native:button"))
    pushed = [event for _, event in Inbox(tmp_path / "discord-sync" / "inbox").pending()]
    assert [(event["kind"], event.get("message", {}).get("content")) for event in pushed] == [
        ("forum", "bridge alive: synced"),
        ("tags", None),
        ("mention", "<@42> start a task"),
        ("forum", "someone else in the forum"),
        ("press", None),
    ]
    assert pushed[1] == {"kind": "tags", "thread": "7", "tags": ["9"]}
    assert pushed[4] == {
        "kind": "press",
        "custom_id": "hds:approve:srq-1:once",
        "user": "217",
        "name": "Quill",
    }
    # Allowed people see 👀 the moment the gateway hands their message over.
    assert seen == [("bridge alive: synced", "👀"), ("<@42> start a task", "👀")]
    assert acknowledged == [("defer", "hds:approve:srq-1:once")]
    os.utime(beat, (1, 1))
    await bot.on_message(message("bridge died: gateway answers", 123))
    # A sync mention never becomes a native thread: it is told the sync is down.
    await bot.on_message(message("<@42> while down", mentions=[bot.user], channel=text_channel))
    assert len(replies) == 1
    assert replies[0][0] == "<@42> while down"
    assert replies[0][1].startswith("discord sync is offline")
    await bot.on_interaction(press("hds:approve:srq-1:once"))
    assert acknowledged[-1] == ("offline", "hds:approve:srq-1:once")
    assert answered_by_gateway == [
        "bridge offline: gateway answers",
        "other forum",
        "normal channel",
        "<@42> stranger",
        "bridge died: gateway answers",
    ]
    beat.touch()
    for dispose in disposers:
        dispose()
    await bot.on_message(message("after unload", 123))
    assert answered_by_gateway[-1] == "after unload"


async def test_each_post_is_tagged_with_where_it_stands(world):
    bridge, external, link = world.bridge, world.external, world.link
    thread = link["thread"]
    world.pins.set("session-one", True)
    await bridge.receive(link, external.add_message(thread, "build it"))
    await through(bridge, "message.start")
    await bridge.tick()
    assert external.tag_names(thread) == ["pinned", "working"]
    approval = {"session_id": "live-one", "command": "deploy", "choices": ["once", "deny"]}
    await bridge.show_request({"id": "srq-1", "method": "approval", "params": approval})
    await bridge.tick()
    assert external.tag_names(thread) == ["needs you", "pinned"]
    await bridge.receive(link, external.add_message(thread, "yes"))
    await bridge.tick()
    assert external.tag_names(thread) == ["pinned", "working"]
    external.rows.append({"row_id": 9, "role": "assistant", "text": "deployed"})
    await external.event("message.complete", {"status": "complete", "text": "deployed"})
    await through(bridge, "message.complete")
    await bridge.tick()
    assert external.tag_names(thread) == ["done", "pinned"]


async def test_a_failed_turn_and_a_turn_running_at_reconnect_are_tagged_once_settled(world):
    bridge, external, link = world.bridge, world.external, world.link
    thread = link["thread"]
    await external.event("message.start")
    await through(bridge, "message.start")
    await bridge.tick()
    assert external.tag_names(thread) == ["working"]
    await external.event("message.complete", {"status": "error", "error": "boom"})
    await through(bridge, "message.complete")
    await bridge.tick()
    assert external.tag_names(thread) == ["needs you"]
    # A finish missed while disconnected must not leave the post "working" forever.
    external.rows.append({"row_id": 1, "role": "assistant", "text": "half done"})
    await external.event("message.start")
    await through(bridge, "message.start")
    await bridge.tick()
    await bridge._connect()
    await bridge.tick()
    assert external.tag_names(thread) == ["done"]


async def test_you_are_pinged_when_something_needs_you_or_your_discord_turn_finishes(world):
    bridge, external, link = world.bridge, world.external, world.link
    thread = link["thread"]

    def pings():
        return [(message["content"], message["notifies"]) for message in external.notified(thread)]

    await bridge.receive(link, external.add_message(thread, "build it"))
    await through(bridge, "message.start")
    approval = {"session_id": "live-one", "command": "deploy", "choices": ["once", "deny"]}
    await bridge.show_request({"id": "srq-1", "method": "approval", "params": approval})
    assert pings() == [("<@42> needs your ok", ["42"])]
    # The card shown again after a reconnect does not ping twice.
    await bridge.show_request({"id": "srq-1", "method": "approval", "params": approval})
    assert len(pings()) == 1
    # Answered: the ping goes away with the need.
    await bridge.receive(link, external.add_message(thread, "yes"))
    assert pings() == []
    external.rows.append({"row_id": 9, "role": "assistant", "text": "deployed"})
    await external.event("message.complete", {"status": "complete", "text": "deployed"})
    await through(bridge, "message.complete")
    assert pings() == [("<@42> done", ["42"])]
    # A turn started at the desktop clears the old ping and finishes quietly.
    await external.event("message.start")
    await through(bridge, "message.start")
    assert pings() == []
    await external.event("message.complete", {"status": "complete", "text": ""})
    await through(bridge, "message.complete")
    assert pings() == []
    # A failure pings whoever started the turn.
    await external.event("message.start")
    await through(bridge, "message.start")
    await external.event("message.complete", {"status": "error", "error": "boom"})
    await through(bridge, "message.complete")
    assert pings() == [("<@42> hit an error", ["42"])]


@pytest.mark.skipif(not HERMES_AGENT.is_dir(), reason="needs a Hermes Agent checkout (HERMES_AGENT_DIR)")
async def test_a_multiplexed_gateway_still_hands_allowed_mentions_to_the_bridge(tmp_path, monkeypatch):
    sys.path.insert(0, str(HERMES_AGENT))
    from agent.secret_scope import reset_multiplex_context, set_multiplex_context

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("DISCORD_ALLOWED_USERS", raising=False)
    (tmp_path / ".env").write_text("DISCORD_ALLOWED_USERS=217\n")
    spec = importlib.util.spec_from_file_location(
        "sync_plugin_multiplexed", PLUGIN_ROOT / "__init__.py", submodule_search_locations=[str(PLUGIN_ROOT)]
    )
    plugin = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = plugin
    spec.loader.exec_module(plugin)
    installers = []
    plugin.register(
        SimpleNamespace(
            get_config=lambda key, default: {"forum_id": "123"}.get(key, default),
            on_unload=lambda dispose: None,
            register_platform_handler=lambda platform, install: installers.append(install),
        )
    )
    answered_by_gateway = []

    class NativeBot:
        user = SimpleNamespace(id=42)

        async def on_message(self, message):
            answered_by_gateway.append(message.content)

        def event(self, handler):
            setattr(self, handler.__name__, handler)
            return handler

    class TextChannel(SimpleNamespace):
        pass

    async def add_reaction(emoji):
        pass

    def mention(content, author):
        return SimpleNamespace(
            channel=TextChannel(id=77, parent_id=None),
            content=content,
            id=5,
            author=SimpleNamespace(id=author, bot=False),
            mentions=[bot.user],
            guild=object(),
            webhook_id=None,
            attachments=[],
            add_reaction=add_reaction,
        )

    bot = NativeBot()
    installers[0](bot, None)
    beat = tmp_path / "discord-sync" / "heartbeat"
    beat.parent.mkdir()
    beat.touch()
    token = set_multiplex_context(True)
    try:
        await bot.on_message(mention("<@42> from quill", "217"))
        await bot.on_message(mention("<@42> from a stranger", "99"))
    finally:
        reset_multiplex_context(token)
    inbox = Inbox(tmp_path / "discord-sync" / "inbox")
    pushed = [event["message"]["content"] for _, event in inbox.pending()]
    assert pushed == ["<@42> from quill"]
    assert answered_by_gateway == ["<@42> from a stranger"]


async def test_each_profile_forum_and_routed_mention_reaches_that_profiles_bridge(tmp_path, monkeypatch):
    sys.path.insert(0, str(HERMES_AGENT))
    # A home of its own, so profiles resolve under this HERMES_HOME wherever the temp dir lives.
    monkeypatch.setenv("HOME", str(tmp_path / "user"))
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("DISCORD_ALLOWED_USERS", "217")
    # The gateway routes the personal text channel to the personal profile.
    (tmp_path / "config.yaml").write_text(
        "gateway:\n  multiplex_profiles: true\n  profile_routes:\n"
        "    - {name: personal, platform: discord, chat_id: '88', profile: personal}\n"
    )
    (tmp_path / "profiles" / "personal").mkdir(parents=True)
    (tmp_path / "profiles" / "personal" / "config.yaml").write_text("{}\n")
    spec = importlib.util.spec_from_file_location(
        "sync_plugin_profiles", PLUGIN_ROOT / "__init__.py", submodule_search_locations=[str(PLUGIN_ROOT)]
    )
    plugin = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = plugin
    spec.loader.exec_module(plugin)
    settings = {"forum_id": "123", "profiles": {"personal": {"forum_id": "456"}}}
    installers = []
    plugin.register(
        SimpleNamespace(
            get_config=lambda key, default: settings.get(key, default),
            on_unload=lambda dispose: None,
            register_platform_handler=lambda platform, install: installers.append(install),
        )
    )
    answered_by_gateway = []
    replies = []

    class NativeBot:
        user = SimpleNamespace(id=42)

        async def on_message(self, message):
            answered_by_gateway.append(message.content)

        def event(self, handler):
            setattr(self, handler.__name__, handler)
            return handler

    class TextChannel(SimpleNamespace):
        pass

    async def add_reaction(emoji):
        pass

    def message(content, channel, mentions=()):
        async def reply(text, mention_author=True):
            replies.append((content, text))

        return SimpleNamespace(
            channel=channel,
            content=content,
            id=5,
            author=SimpleNamespace(id="217", bot=False),
            mentions=list(mentions),
            guild=SimpleNamespace(id=1),
            webhook_id=None,
            attachments=[],
            add_reaction=add_reaction,
            reply=reply,
        )

    async def defer():
        pass

    bot = NativeBot()
    installers[0](bot, None)
    work_dir = tmp_path / "discord-sync"
    personal_dir = tmp_path / "profiles" / "personal" / "discord-sync"
    for sync_dir in (work_dir, personal_dir):
        sync_dir.mkdir(parents=True)
        (sync_dir / "heartbeat").touch()
    personal_channel = TextChannel(id=88, parent_id=None)
    work_channel = TextChannel(id=77, parent_id=None)
    await bot.on_message(message("work post", SimpleNamespace(id=1, parent_id=123)))
    await bot.on_message(message("personal post", SimpleNamespace(id=2, parent_id=456)))
    await bot.on_message(message("<@42> work task", work_channel, [bot.user]))
    await bot.on_message(message("<@42> personal task", personal_channel, [bot.user]))
    await bot.on_message(message("plain personal chat", personal_channel))
    await bot.on_raw_thread_update(SimpleNamespace(data={"id": 2, "parent_id": 456, "applied_tags": [9]}))
    await bot.on_interaction(
        SimpleNamespace(
            data={"custom_id": "hds:approve:srq-1:once"},
            user=SimpleNamespace(id="217", display_name="Quill"),
            channel=SimpleNamespace(id=2, parent_id=456),
            response=SimpleNamespace(defer=defer),
        )
    )

    def pushed(sync_dir):
        events = [event for _, event in Inbox(sync_dir / "inbox").pending()]
        return [(event["kind"], event.get("message", {}).get("content")) for event in events]

    assert pushed(work_dir) == [("forum", "work post"), ("mention", "<@42> work task")]
    assert pushed(personal_dir) == [
        ("forum", "personal post"),
        ("mention", "<@42> personal task"),
        ("tags", None),
        ("press", None),
    ]
    assert answered_by_gateway == ["plain personal chat"]
    # With only the personal bridge down, its mention is refused and its forum goes back to the gateway.
    os.utime(personal_dir / "heartbeat", (1, 1))
    await bot.on_message(message("<@42> personal while down", personal_channel, [bot.user]))
    await bot.on_message(message("personal post while down", SimpleNamespace(id=2, parent_id=456)))
    await bot.on_message(message("work post still synced", SimpleNamespace(id=1, parent_id=123)))
    assert [content for content, _ in replies] == ["<@42> personal while down"]
    assert answered_by_gateway[-1] == "personal post while down"
    assert pushed(work_dir)[-1] == ("forum", "work post still synced")


async def poll(world):
    world.clock.advance(POLL_EVERY)
    await world.bridge.tick()


async def test_closing_a_post_archives_its_session_and_deletes_the_post(world):
    bridge, external, thread = world.bridge, world.external, world.link["thread"]
    await bridge.tick()
    external.close_post(thread, ago=AUDIT_LAG)
    world.inbox.put({"kind": "tags", "thread": thread, "tags": []})
    await bridge.tick()
    assert external.archived == {"session-one": True}
    await bridge.tick()
    assert thread not in external.threads


async def test_a_close_the_push_missed_is_caught_by_the_poll(world):
    external, link = world.external, world.link
    external.close_post(link["thread"])
    # A retag due before the bridge sees the close waits instead of failing the pass.
    world.pins.set("session-one", True)
    await poll(world)
    assert external.archived == {"session-one": True}
    assert external.threads[link["thread"]]["archived"]


async def test_idle_archives_and_closes_by_others_reopen_without_hiding_the_session(world):
    external, thread = world.external, world.link["thread"]
    external.close_post(thread, ago=60, logged=False)
    await poll(world)
    assert not external.threads[thread]["archived"]
    external.close_post(thread, user="99", ago=60)
    await poll(world)
    assert not external.threads[thread]["archived"]
    assert external.archived == {}


async def test_a_fresh_archive_waits_for_its_audit_entry_before_the_poll_reopens_it(world):
    external, thread = world.external, world.link["thread"]
    external.close_post(thread, logged=False)
    await poll(world)
    assert external.threads[thread]["archived"]
    assert external.archived == {}


async def test_archiving_a_session_anywhere_deletes_its_post_and_unarchiving_reposts_its_history(world):
    bridge, external, link = world.bridge, world.external, world.link
    desktop = Rpc(world.http, world.base + "/api/ws")
    await desktop.connect()
    try:
        snapshot = await desktop.call("session.resume", {"session_id": "session-one"})
        await desktop.call("prompt.submit", {"session_id": snapshot["session_id"], "text": "keep this"})
    finally:
        await desktop.close()
    await bridge.reconcile(link)
    # Desktop, an agent or the idle sweep: each one only sets the session's archived flag.
    external.archived["session-one"] = True
    await bridge.tick()
    assert link["thread"] not in external.threads
    for _ in range(3):
        await poll(world)
    assert external.archived == {"session-one": True}
    external.archived["session-one"] = False
    await bridge.tick()
    [reposted] = bridge.store.links()
    assert reposted["thread"] != link["thread"]
    assert "you: keep this" in bodies(external, reposted["thread"])


async def test_a_post_discord_refuses_to_delete_is_retried(world):
    bridge, external, thread = world.bridge, world.external, world.link["thread"]
    external.archived["session-one"] = True
    external.refuse_deletes = True
    await bridge.tick()
    assert thread in external.threads
    external.refuse_deletes = False
    await bridge.tick()
    assert thread not in external.threads

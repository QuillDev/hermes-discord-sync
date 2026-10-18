"""What the native Hermes Discord adapter says, reused for the forum.

Inside Hermes the catalog strings (``agent.i18n``) and tool emoji (``agent.display``)
are used, so the forum reads like the native bot. Outside Hermes (tests) the
English catalog values below stand in.
"""

from __future__ import annotations

# Reactions on the user's own message, as the native adapter uses them.
SEEN = "👀"
DONE = "✅"
FAILED = "❌"
STOPPED = "⏹️"
STEERED = "⏩"
REDIRECTED = "↪️"
QUEUED = "⏳"

_ENGLISH = {
    "gateway.busy.steered_head": "⏩ Steered into current run",
    "gateway.busy.steered_tail": ". Your message arrives after the next tool call.",
    "gateway.busy.redirected_head": "↪ Redirected current run",
    "gateway.busy.redirected_tail": ". I'll adjust using your correction.",
    "gateway.busy.queued_head": "⏳ Queued for the next turn",
    "gateway.busy.queued_tail": ". I'll respond once the current task finishes.",
    "gateway.exec_approval.header": "Hermes wants to run a command that needs your OK",
    "gateway.exec_approval.action_once": "Allow Once",
    "gateway.exec_approval.action_session": "Allow Session",
    "gateway.exec_approval.action_always": "Always Allow",
    "gateway.exec_approval.action_deny": "Deny",
    "platform.discord.approval.resolved_once": "Approved once",
    "platform.discord.approval.resolved_session": "Approved for session",
    "platform.discord.approval.resolved_always": "Approved permanently",
    "platform.discord.approval.resolved_deny": "Denied",
    "platform.discord.approval.expired": (
        "⌛ Approval expired — command was not run (already timed out or resolved elsewhere)"
    ),
    "platform.discord.approval.by_user": "{label} by {user}",
    "platform.discord.prompt.clarify_title": "Hermes needs your input",
    "platform.discord.prompt.clarify_hint_text": "Reply in this channel with your answer.",
}

# Discord button styles.
PRIMARY, SECONDARY, SUCCESS, DANGER = 1, 2, 3, 4
BUTTON_LABEL_LIMIT = 80

APPROVAL_BUTTONS = {
    "once": ("gateway.exec_approval.action_once", SUCCESS, "platform.discord.approval.resolved_once"),
    "session": (
        "gateway.exec_approval.action_session",
        SECONDARY,
        "platform.discord.approval.resolved_session",
    ),
    "always": ("gateway.exec_approval.action_always", PRIMARY, "platform.discord.approval.resolved_always"),
    "deny": ("gateway.exec_approval.action_deny", DANGER, "platform.discord.approval.resolved_deny"),
}

ACK_KEYS = {
    "steered": ("gateway.busy.steered_head", "gateway.busy.steered_tail"),
    "redirected": ("gateway.busy.redirected_head", "gateway.busy.redirected_tail"),
    "queued": ("gateway.busy.queued_head", "gateway.busy.queued_tail"),
}

STATUS_REACTIONS = {"steered": STEERED, "redirected": REDIRECTED, "queued": QUEUED}


class Wording:
    def text(self, key, **values):
        return _ENGLISH[key].format(**values)

    def tool_emoji(self, name) -> str:
        return "⚙️"

    def ack(self, status):
        """The native busy acknowledgement for a prompt.submit status, or None for a new turn."""
        keys = ACK_KEYS.get(status)
        if keys is None:
            return None
        head, tail = keys
        return self.text(head) + self.text(tail)

    def approval_label(self, choice):
        known = APPROVAL_BUTTONS.get(choice)
        return self.text(known[0]) if known else choice

    def approval_style(self, choice):
        known = APPROVAL_BUTTONS.get(choice)
        return known[1] if known else SECONDARY

    def approval_outcome(self, choice):
        known = APPROVAL_BUTTONS.get(choice)
        return self.text(known[2]) if known else choice


class HermesWording(Wording):
    """The running Hermes catalog, so wording and language match the native bot."""

    def text(self, key, **values):
        from agent.i18n import t

        value = t(key, **values)
        # t() echoes the key when the catalog lacks it.
        return super().text(key, **values) if value == key else value

    def tool_emoji(self, name) -> str:
        from agent.display import get_tool_emoji

        return get_tool_emoji(name, default="⚙️")

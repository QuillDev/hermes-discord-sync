"""What the agent this plugin is loaded into calls itself.

A rebranded build of the agent renames its core modules, so the name is read off one of them rather
than written here: what the forum shows people names the agent they are actually running.
"""

from __future__ import annotations


def _name() -> str:
    try:
        import hermes_constants
    except ImportError:  # outside the agent (tests)
        return "hermes"
    return hermes_constants.__name__.partition("_")[0]


NAME = _name()

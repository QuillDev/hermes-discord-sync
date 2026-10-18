"""Gateway-to-bridge handoff: one file per event, written atomically, read in order."""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path


class Inbox:
    def __init__(self, directory):
        self.directory = Path(directory)

    def put(self, event):
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        name = f"{time.time_ns():020d}-{uuid.uuid4().hex}"
        # Write then rename, so a reader never sees a half-written event.
        temporary = self.directory / f".{name}.tmp"
        temporary.write_text(json.dumps(event))
        os.replace(temporary, self.directory / f"{name}.json")

    def pending(self):
        """Events oldest first, as (handle, event). Call done(handle) once one is handled."""
        if not self.directory.is_dir():
            return []
        paths = sorted(self.directory.glob("*.json"))
        return [(path, json.loads(path.read_text())) for path in paths]

    def done(self, handle):
        handle.unlink(missing_ok=True)

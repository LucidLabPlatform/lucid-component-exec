"""
allowlist.py — command allow-list enforcement for lucid-component-exec.

The allow-list is a sequence of glob-style patterns applied against the
*raw command string* received in the MQTT payload.  A command is accepted
if it matches at least one pattern; rejected otherwise.

Pattern semantics (fnmatch):
  *      matches any sequence of characters within a single path component
  ?      matches any single character
  [seq]  matches any character in seq

Examples
--------
  "systemctl *"         → any systemctl sub-command
  "ls *"               → ls with any arguments
  "echo *"             → echo anything
  ""                   → exact empty-string match (rarely useful)

When the allow-list is empty every command is permitted (open mode).
Central Command or the component registry config can populate this list
to lock down what operators may run on edge devices.
"""

from __future__ import annotations

import fnmatch
from typing import Sequence


def is_allowed(command: str, patterns: Sequence[str]) -> bool:
    """
    Return True if *command* matches at least one pattern in *patterns*.

    If *patterns* is empty, all commands are permitted.

    Parameters
    ----------
    command:
        The raw command string as received in the MQTT payload.
    patterns:
        Sequence of fnmatch-style glob patterns to test against.
    """
    if not patterns:
        return True
    for pattern in patterns:
        if fnmatch.fnmatch(command, pattern):
            return True
    return False

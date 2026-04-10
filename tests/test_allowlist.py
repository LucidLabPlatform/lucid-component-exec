"""
Unit tests for lucid_component_exec.allowlist.

Verifies glob matching logic without any subprocess or MQTT involvement.
"""
from __future__ import annotations

import pytest

from lucid_component_exec.allowlist import is_allowed


# ---------------------------------------------------------------------------
# Open mode (empty allow-list)
# ---------------------------------------------------------------------------


def test_empty_allow_list_permits_any_command():
    assert is_allowed("echo hello", []) is True
    assert is_allowed("rm -rf /", []) is True
    assert is_allowed("", []) is True


# ---------------------------------------------------------------------------
# Exact match
# ---------------------------------------------------------------------------


def test_exact_match_returns_true():
    assert is_allowed("echo hello", ["echo hello"]) is True


def test_no_match_returns_false():
    assert is_allowed("rm -rf /", ["echo *"]) is False


# ---------------------------------------------------------------------------
# Glob patterns
# ---------------------------------------------------------------------------


def test_wildcard_matches_any_suffix():
    assert is_allowed("systemctl restart nginx", ["systemctl *"]) is True
    assert is_allowed("systemctl status sshd", ["systemctl *"]) is True


def test_wildcard_does_not_match_different_prefix():
    assert is_allowed("journalctl -u nginx", ["systemctl *"]) is False


def test_question_mark_matches_single_char():
    assert is_allowed("ls -l", ["ls -?"]) is True
    assert is_allowed("ls -la", ["ls -?"]) is False


def test_multiple_patterns_first_match_wins():
    patterns = ["echo *", "ls *", "systemctl *"]
    assert is_allowed("ls /tmp", patterns) is True
    assert is_allowed("systemctl status", patterns) is True
    assert is_allowed("rm /etc/passwd", patterns) is False


def test_character_class_pattern():
    assert is_allowed("ls -l", ["ls -[la]"]) is True
    assert is_allowed("ls -a", ["ls -[la]"]) is True
    assert is_allowed("ls -r", ["ls -[la]"]) is False


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_command_with_exact_empty_pattern():
    assert is_allowed("", [""]) is True


def test_empty_command_with_wildcard():
    # fnmatch("", "*") == True
    assert is_allowed("", ["*"]) is True


def test_command_not_in_multi_pattern_list():
    patterns = ["echo *", "ping *"]
    assert is_allowed("wget http://example.com", patterns) is False


def test_non_list_patterns_sequence_tuple():
    # Accepts any Sequence[str], including tuple.
    assert is_allowed("echo hi", ("echo *",)) is True

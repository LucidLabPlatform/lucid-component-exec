"""
Unit tests for lucid_component_exec.executor.

These tests invoke real subprocesses using portable shell commands (echo, sh -c
etc.) that are available on Linux and macOS.  No MQTT or Component machinery is
involved.
"""
from __future__ import annotations

import sys

import pytest

from lucid_component_exec.executor import MAX_OUTPUT_BYTES, run


# ---------------------------------------------------------------------------
# Basic execution
# ---------------------------------------------------------------------------


def test_run_echo_returns_ok():
    result = run("echo hello")
    assert result["exit_code"] == 0
    assert result["stdout"].strip() == "hello"
    assert result["stderr"] == ""
    assert result["timed_out"] is False
    assert result["error"] is None


def test_run_captures_stderr():
    result = run("echo err_message >&2")
    assert result["exit_code"] == 0
    assert "err_message" in result["stderr"]


def test_run_nonzero_exit_code():
    result = run("exit 42", shell=True)
    assert result["exit_code"] == 42
    assert result["timed_out"] is False
    assert result["error"] is None


def test_run_multiline_output():
    result = run("printf 'line1\\nline2\\nline3'")
    assert result["exit_code"] == 0
    lines = result["stdout"].strip().splitlines()
    assert lines == ["line1", "line2", "line3"]


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------


def test_run_timeout_sets_timed_out_flag():
    result = run("sleep 60", timeout_s=0.3)
    assert result["timed_out"] is True
    assert result["exit_code"] is None
    assert result["error"] is not None
    assert "timed out" in result["error"].lower()


# ---------------------------------------------------------------------------
# Environment overlay
# ---------------------------------------------------------------------------


def test_run_env_overlay_visible_in_subprocess():
    result = run("echo $MY_CUSTOM_VAR", env_overlay={"MY_CUSTOM_VAR": "lucid_test"})
    assert result["exit_code"] == 0
    assert "lucid_test" in result["stdout"]


def test_run_no_env_overlay_still_has_path():
    # PATH must be inherited so basic commands still resolve.
    result = run("echo $PATH")
    assert result["exit_code"] == 0
    assert result["stdout"].strip()  # non-empty


# ---------------------------------------------------------------------------
# Working directory
# ---------------------------------------------------------------------------


def test_run_cwd_changes_working_directory(tmp_path):
    # pwd should output the tmp directory path.
    result = run("pwd", cwd=str(tmp_path))
    assert result["exit_code"] == 0
    # Resolve both to handle macOS /private/… symlinks.
    import os
    assert os.path.realpath(result["stdout"].strip()) == os.path.realpath(str(tmp_path))


# ---------------------------------------------------------------------------
# Output truncation
# ---------------------------------------------------------------------------


def test_run_stdout_is_truncated_when_too_large():
    # Generate more than MAX_OUTPUT_BYTES of output.
    byte_count = MAX_OUTPUT_BYTES + 500
    result = run(f"python3 -c \"print('A' * {byte_count})\"")
    assert len(result["stdout"]) <= MAX_OUTPUT_BYTES + 60  # truncation message adds chars
    assert "truncated" in result["stdout"]


# ---------------------------------------------------------------------------
# OS-level failure (shell=False with nonexistent binary)
# ---------------------------------------------------------------------------


def test_run_shell_false_nonexistent_binary():
    result = run("/this/binary/does/not/exist/lucid_test_xyz", shell=False)
    assert result["exit_code"] is None
    assert result["error"] is not None
    assert result["timed_out"] is False

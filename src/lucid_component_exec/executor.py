"""
executor.py — subprocess execution logic for lucid-component-exec.

Isolated here so it can be unit-tested independently of the Component lifecycle
and so the component module stays free of subprocess bookkeeping.

Design decisions:
- Each run() call is blocking and returns a structured result dict.
- Caller is responsible for running in a thread to avoid blocking the MQTT loop.
- stdout/stderr are captured separately and returned as strings (truncated at
  MAX_OUTPUT_BYTES to keep MQTT payloads bounded).
- The working directory, environment overlay, and timeout are supplied by the
  caller; executor never reads config directly.
- A non-zero exit code is NOT treated as an error at this layer — the component
  layer decides how to map exit codes to ok/error in the result topic.
"""

from __future__ import annotations

import os
import subprocess
from typing import Any

# Hard cap on combined stdout + stderr carried in the MQTT result payload.
# WS transport packets can hold tens of KB; keep well within that.
MAX_OUTPUT_BYTES = 16_384  # 16 KiB per stream


def run(
    command: str,
    *,
    shell: bool = True,
    timeout_s: float = 30.0,
    cwd: str | None = None,
    env_overlay: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Execute *command* in a subprocess and return a structured result.

    Parameters
    ----------
    command:
        Shell command string (passed to ``/bin/sh -c`` when shell=True) or a
        single executable path (shell=False, no argument splitting).
    shell:
        Whether to execute via the shell.  Defaults to True.  Pass False only
        for trusted, pre-split argument lists — the component layer validates
        the allow-list before calling here.
    timeout_s:
        Wall-clock timeout in seconds.  Process is killed (SIGKILL) on breach.
    cwd:
        Working directory for the subprocess.  None means the agent process cwd.
    env_overlay:
        If provided, these key=value pairs are merged on top of the current
        process environment before the subprocess is started.

    Returns
    -------
    dict with keys:
        exit_code   int       Return code; None if process was killed by timeout.
        stdout      str       Captured standard output (may be truncated).
        stderr      str       Captured standard error (may be truncated).
        timed_out   bool      True when the timeout was exceeded and the process
                              was forcefully terminated.
        error       str|None  Exception message when the OS failed to start the
                              process at all (e.g. command not found with shell=False).
    """
    env: dict[str, str] | None = None
    if env_overlay:
        env = {**os.environ, **env_overlay}

    try:
        proc = subprocess.run(
            command,
            shell=shell,
            capture_output=True,
            timeout=timeout_s,
            cwd=cwd,
            env=env,
        )
        stdout = proc.stdout.decode("utf-8", errors="replace")
        stderr = proc.stderr.decode("utf-8", errors="replace")

        # Truncate to avoid oversized MQTT payloads.
        if len(stdout) > MAX_OUTPUT_BYTES:
            stdout = stdout[:MAX_OUTPUT_BYTES] + f"\n[truncated at {MAX_OUTPUT_BYTES} bytes]"
        if len(stderr) > MAX_OUTPUT_BYTES:
            stderr = stderr[:MAX_OUTPUT_BYTES] + f"\n[truncated at {MAX_OUTPUT_BYTES} bytes]"

        return {
            "exit_code": proc.returncode,
            "stdout": stdout,
            "stderr": stderr,
            "timed_out": False,
            "error": None,
        }

    except subprocess.TimeoutExpired as exc:
        # subprocess.run kills the child on TimeoutExpired; stdout/stderr may be
        # partially captured in the exception.
        stdout_raw = exc.stdout or b""
        stderr_raw = exc.stderr or b""
        stdout = stdout_raw.decode("utf-8", errors="replace")[:MAX_OUTPUT_BYTES]
        stderr = stderr_raw.decode("utf-8", errors="replace")[:MAX_OUTPUT_BYTES]
        return {
            "exit_code": None,
            "stdout": stdout,
            "stderr": stderr,
            "timed_out": True,
            "error": f"timed out after {timeout_s}s",
        }

    except Exception as exc:  # noqa: BLE001  # OS-level failures (ENOENT, EPERM…)
        return {
            "exit_code": None,
            "stdout": "",
            "stderr": "",
            "timed_out": False,
            "error": str(exc),
        }

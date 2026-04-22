"""
ExecComponent — LUCID component for controlled shell-command execution.

Allows Central Command (or any authorised MQTT publisher) to execute shell
commands on an agent device and receive the results via MQTT.

MQTT contract (component_id = "exec"):

  Retained (QoS 1):
    .../components/exec/metadata
    .../components/exec/status
    .../components/exec/state
    .../components/exec/cfg
    .../components/exec/cfg/logging
    .../components/exec/cfg/telemetry

  Streams (QoS 0):
    .../components/exec/logs
    .../components/exec/telemetry/run_count   (total completed runs since start)

  Commands:
    .../components/exec/cmd/run
      payload: {
        "request_id": "<uuid>",
        "command":    "<shell command string>",
        "timeout_s":  <float, optional, default from cfg>,
        "cwd":        "<path, optional>",
        "env":        {"KEY": "VALUE", ...}  (optional overlay, merged on top of agent env)
      }

    Standard commands (handled by base):
      cmd/reset, cmd/ping,
      cmd/cfg/set, cmd/cfg/logging/set, cmd/cfg/telemetry/set

  Result events (QoS 1):
    .../components/exec/evt/run/result
      payload: {
        "request_id": "<uuid>",
        "ok":         <bool>,    # True unless allow-list rejected or OS failed to start process
        "error":      "<str>" | null,
        "exit_code":  <int> | null,
        "stdout":     "<str>",
        "stderr":     "<str>",
        "timed_out":  <bool>
      }

    .../components/exec/evt/reset/result
    .../components/exec/evt/ping/result
    .../components/exec/evt/cfg/set/result
    .../components/exec/evt/cfg/logging/set/result
    .../components/exec/evt/cfg/telemetry/set/result

Configuration (cfg topic / registry config):
  allow_list      list[str]   Glob patterns for permitted commands.
                              Empty list = open (all commands allowed).
  default_timeout_s float     Default subprocess timeout in seconds.  [30.0]
  max_timeout_s   float       Upper bound on per-request timeout.      [300.0]
  cwd             str|null    Default working directory for subprocesses.

Security model:
  - The allow-list is the primary safety control.  Configure it in the
    component registry before deploying to production devices.
  - Commands rejected by the allow-list never reach subprocess.run().
  - timeout_s from the payload is clamped to [1, max_timeout_s].
  - Per-request env overlays are merged on top of (not replacing) the agent
    process environment, so PATH, HOME etc. are always available.
  - No privilege escalation is performed; the component runs as the same
    OS user as the agent process.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import threading
import time
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from lucid_component_base import Component, ComponentContext

from . import allowlist as _allowlist
from . import executor as _executor


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_DEFAULT_TIMEOUT_S: float = 30.0
_DEFAULT_MAX_TIMEOUT_S: float = 300.0
_MAX_SPAWNED_PROCESSES: int = 20
_SPAWN_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
_DEFAULT_SIGTERM_TIMEOUT_S: float = 10.0


class ExecComponent(Component):
    """
    LUCID component that executes allow-listed shell commands on the agent device
    and publishes structured results to evt/run/result.

    All runs are dispatched to a thread pool so the MQTT callback thread is never
    blocked.  Concurrent runs are allowed up to _MAX_CONCURRENT_RUNS; excess
    requests are rejected with a descriptive error result.
    """

    _MAX_CONCURRENT_RUNS: int = 4

    def __init__(self, context: ComponentContext) -> None:
        super().__init__(context)
        self._log = context.logger()

        cfg = context.config

        # Allow-list: list of fnmatch glob patterns.  Empty = open.
        raw_patterns = cfg.get("allow_list", [])
        self._allow_list: list[str] = list(raw_patterns) if isinstance(raw_patterns, list) else []

        self._default_timeout_s: float = float(cfg.get("default_timeout_s", _DEFAULT_TIMEOUT_S))
        self._max_timeout_s: float = float(cfg.get("max_timeout_s", _DEFAULT_MAX_TIMEOUT_S))
        self._default_cwd: str | None = cfg.get("cwd") or None

        # Concurrency tracking
        self._active_runs: int = 0
        self._active_runs_lock = threading.Lock()

        # Telemetry counter — total runs completed (started and finished, regardless of exit code).
        self._run_count: int = 0
        self._run_count_lock = threading.Lock()

        # Managed long-running (detached) processes, keyed by user-supplied name.
        # Each entry: {"proc": Popen, "pid": int, "command": str, "cwd": str|None,
        #              "started_at": iso8601, "exit_code": int|None, "killing": bool}
        self._spawned: dict[str, dict[str, Any]] = {}
        self._spawned_lock = threading.Lock()

    # ------------------------------------------------------------------
    # LUCID component contract
    # ------------------------------------------------------------------

    @property
    def component_id(self) -> str:
        return "exec"

    def capabilities(self) -> list[str]:
        return ["reset", "ping", "run", "spawn", "kill", "list"]

    def get_cfg_payload(self) -> dict[str, Any]:
        return {
            "allow_list": self._allow_list,
            "default_timeout_s": self._default_timeout_s,
            "max_timeout_s": self._max_timeout_s,
            "cwd": self._default_cwd,
        }

    def get_state_payload(self) -> dict[str, Any]:
        with self._run_count_lock:
            run_count = self._run_count
        with self._active_runs_lock:
            active = self._active_runs
        with self._spawned_lock:
            spawned = [
                {
                    "name": name,
                    "pid": entry["pid"],
                    "started_at": entry["started_at"],
                    "alive": entry["exit_code"] is None,
                    "exit_code": entry["exit_code"],
                }
                for name, entry in self._spawned.items()
            ]
        return {
            "active_runs": active,
            "run_count": run_count,
            "allow_list_size": len(self._allow_list),
            "allow_list_open": len(self._allow_list) == 0,
            "default_timeout_s": self._default_timeout_s,
            "max_timeout_s": self._max_timeout_s,
            "spawned": spawned,
            "spawned_alive": sum(1 for s in spawned if s["alive"]),
        }

    def schema(self) -> dict[str, Any]:
        s = deepcopy(super().schema())

        # -- publishes --
        s["publishes"]["state"]["fields"].update({
            "active_runs": {"type": "integer"},
            "run_count": {"type": "integer"},
            "allow_list_size": {"type": "integer"},
            "allow_list_open": {"type": "boolean"},
            "default_timeout_s": {"type": "float"},
            "max_timeout_s": {"type": "float"},
            "spawned": {
                "type": "array",
                "description": "Managed long-running processes",
            },
            "spawned_alive": {"type": "integer"},
        })

        s["publishes"]["cfg"]["fields"].update({
            "allow_list": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Glob patterns for allowed commands. Empty = open",
            },
            "default_timeout_s": {"type": "float", "min": 1},
            "max_timeout_s": {"type": "float", "min": 1},
            "cwd": {"type": "string"},
        })

        s["publishes"]["cfg/telemetry"]["fields"].update({
            "run_count": {
                "type": "object",
                "fields": {
                    "enabled": {"type": "boolean"},
                    "interval_s": {"type": "integer", "min": 1},
                    "change_threshold_percent": {"type": "float", "min": 0},
                },
            },
        })

        s["publishes"]["telemetry/run_count"] = {
            "fields": {"value": {"type": "integer"}},
        }

        # -- subscribes --
        s["subscribes"]["cmd/run"] = {
            "fields": {
                "command": {"type": "string", "description": "Shell command to execute"},
                "timeout_s": {"type": "float", "min": 1, "max": 300},
                "cwd": {"type": "string"},
                "env": {"type": "object", "description": "Environment variable overrides"},
            },
        }

        s["subscribes"]["cmd/spawn"] = {
            "fields": {
                "name": {"type": "string", "description": "Unique handle for this process"},
                "command": {"type": "string", "description": "Shell command to spawn (long-running)"},
                "cwd": {"type": "string"},
                "env": {"type": "object", "description": "Environment variable overrides"},
            },
        }

        s["subscribes"]["cmd/kill"] = {
            "fields": {
                "name": {"type": "string", "description": "Name of a spawned process"},
                "sigterm_timeout_s": {"type": "float", "min": 0, "description": "Grace period before SIGKILL"},
            },
        }

        s["subscribes"]["cmd/list"] = {"fields": {}}

        s["subscribes"]["cmd/cfg/set"] = {
            "fields": {
                "set": {
                    "type": "object",
                    "fields": {
                        "allow_list": {"type": "array", "items": {"type": "string"}},
                        "default_timeout_s": {"type": "float", "min": 1},
                        "max_timeout_s": {"type": "float", "min": 1},
                        "cwd": {"type": "string"},
                    },
                },
            },
        }

        return s

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _start(self) -> None:
        self._run_count = 0
        self._active_runs = 0
        self._spawned = {}
        self._log.info(
            "ExecComponent started. allow_list=%r default_timeout_s=%.1f max_timeout_s=%.1f cwd=%r",
            self._allow_list,
            self._default_timeout_s,
            self._max_timeout_s,
            self._default_cwd,
        )
        self.set_telemetry_config({
            "run_count": {
                "enabled": False,
                "interval_s": 0.1,
                "change_threshold_percent": 0.0,
            }
        })
        self._publish_all_retained()

    def _publish_all_retained(self) -> None:
        self.publish_metadata()
        self.publish_schema()
        self.publish_status()
        self.publish_state()
        self.publish_cfg()

    # ------------------------------------------------------------------
    # Standard commands
    # ------------------------------------------------------------------

    def on_cmd_reset(self, payload_str: str) -> None:
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "") if isinstance(payload, dict) else ""
        except json.JSONDecodeError:
            request_id = ""

        self._log.info("cmd/reset request_id=%s", request_id)
        with self._run_count_lock:
            self._run_count = 0
        self.publish_state()
        self.publish_result("reset", request_id, ok=True, error=None)

    def on_cmd_ping(self, payload_str: str) -> None:
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = payload.get("request_id", "") if isinstance(payload, dict) else ""
        except json.JSONDecodeError:
            request_id = ""

        self._log.info("cmd/ping request_id=%s", request_id)
        self.publish_result("ping", request_id, ok=True, error=None)

    def on_cmd_cfg_set(self, payload_str: str) -> None:
        request_id, set_dict, parse_error = self._parse_cfg_set_payload(payload_str)
        if parse_error:
            self.publish_cfg_set_result(
                request_id=request_id,
                ok=False,
                applied=None,
                error=parse_error,
                ts=_utc_iso(),
                action="cfg/set",
            )
            return

        allowed_keys = {"allow_list", "default_timeout_s", "max_timeout_s", "cwd"}
        unknown = sorted(k for k in set_dict if k not in allowed_keys)
        if unknown:
            self.publish_cfg_set_result(
                request_id=request_id,
                ok=False,
                applied=None,
                error=f"unknown cfg key(s): {', '.join(unknown)}",
                ts=_utc_iso(),
                action="cfg/set",
            )
            return

        applied: dict[str, Any] = {}

        if "allow_list" in set_dict:
            raw = set_dict["allow_list"]
            if not isinstance(raw, list):
                self.publish_cfg_set_result(
                    request_id=request_id,
                    ok=False,
                    applied=None,
                    error="allow_list must be a JSON array of strings",
                    ts=_utc_iso(),
                    action="cfg/set",
                )
                return
            self._allow_list = [str(p) for p in raw]
            applied["allow_list"] = self._allow_list

        if "default_timeout_s" in set_dict:
            try:
                val = float(set_dict["default_timeout_s"])
                if val <= 0:
                    raise ValueError("must be positive")
                self._default_timeout_s = val
                applied["default_timeout_s"] = self._default_timeout_s
            except (TypeError, ValueError) as exc:
                self.publish_cfg_set_result(
                    request_id=request_id,
                    ok=False,
                    applied=None,
                    error=f"default_timeout_s: {exc}",
                    ts=_utc_iso(),
                    action="cfg/set",
                )
                return

        if "max_timeout_s" in set_dict:
            try:
                val = float(set_dict["max_timeout_s"])
                if val <= 0:
                    raise ValueError("must be positive")
                self._max_timeout_s = val
                applied["max_timeout_s"] = self._max_timeout_s
            except (TypeError, ValueError) as exc:
                self.publish_cfg_set_result(
                    request_id=request_id,
                    ok=False,
                    applied=None,
                    error=f"max_timeout_s: {exc}",
                    ts=_utc_iso(),
                    action="cfg/set",
                )
                return

        if "cwd" in set_dict:
            cwd_val = set_dict["cwd"]
            self._default_cwd = str(cwd_val) if cwd_val is not None else None
            applied["cwd"] = self._default_cwd

        self.publish_cfg_general()
        self.publish_state()
        self.publish_cfg_set_result(
            request_id=request_id,
            ok=True,
            applied=applied if applied else None,
            error=None,
            ts=_utc_iso(),
            action="cfg/set",
        )

    def on_cmd_cfg_logging_set(self, payload_str: str) -> None:
        self._log.info("cmd/cfg/logging/set")
        super().on_cmd_cfg_logging_set(payload_str)

    # ------------------------------------------------------------------
    # Run command
    # ------------------------------------------------------------------

    def on_cmd_run(self, payload_str: str) -> None:
        """
        Handle cmd/run — validate payload, enforce allow-list, then dispatch
        subprocess execution to a background thread.

        Expected payload:
            {
              "request_id": "<uuid>",
              "command":    "<shell command string>",
              "timeout_s":  <float, optional>,
              "cwd":        "<path, optional>",
              "env":        {"KEY": "VALUE", ...}  (optional)
            }
        """
        try:
            payload = json.loads(payload_str) if payload_str else {}
        except json.JSONDecodeError:
            self._publish_run_result(
                request_id="",
                ok=False,
                error="invalid JSON in payload",
                exit_code=None,
                stdout="",
                stderr="",
                timed_out=False,
            )
            return

        if not isinstance(payload, dict):
            self._publish_run_result(
                request_id="",
                ok=False,
                error="payload must be a JSON object",
                exit_code=None,
                stdout="",
                stderr="",
                timed_out=False,
            )
            return

        request_id: str = payload.get("request_id", "")
        command = payload.get("command")

        if not command or not isinstance(command, str):
            self._publish_run_result(
                request_id=request_id,
                ok=False,
                error="'command' field is required and must be a non-empty string",
                exit_code=None,
                stdout="",
                stderr="",
                timed_out=False,
            )
            return

        command = command.strip()
        if not command:
            self._publish_run_result(
                request_id=request_id,
                ok=False,
                error="'command' must not be empty or whitespace-only",
                exit_code=None,
                stdout="",
                stderr="",
                timed_out=False,
            )
            return

        # Allow-list gate
        if not _allowlist.is_allowed(command, self._allow_list):
            self._log.warning(
                "cmd/run rejected by allow-list. request_id=%s command=%r", request_id, command
            )
            self._publish_run_result(
                request_id=request_id,
                ok=False,
                error="command rejected by allow-list",
                exit_code=None,
                stdout="",
                stderr="",
                timed_out=False,
            )
            return

        # Resolve per-request timeout, clamped to [1, max_timeout_s].
        raw_timeout = payload.get("timeout_s", self._default_timeout_s)
        try:
            timeout_s = float(raw_timeout)
        except (TypeError, ValueError):
            timeout_s = self._default_timeout_s
        timeout_s = max(1.0, min(timeout_s, self._max_timeout_s))

        cwd: str | None = payload.get("cwd") or self._default_cwd

        env_overlay_raw = payload.get("env")
        env_overlay: dict[str, str] | None = None
        if isinstance(env_overlay_raw, dict):
            # Stringify all values for safety
            env_overlay = {str(k): str(v) for k, v in env_overlay_raw.items()}

        # Concurrency gate
        with self._active_runs_lock:
            if self._active_runs >= self._MAX_CONCURRENT_RUNS:
                self._log.warning(
                    "cmd/run rejected: concurrency limit (%d) reached. request_id=%s",
                    self._MAX_CONCURRENT_RUNS,
                    request_id,
                )
                self._publish_run_result(
                    request_id=request_id,
                    ok=False,
                    error=f"concurrency limit reached ({self._MAX_CONCURRENT_RUNS} runs active)",
                    exit_code=None,
                    stdout="",
                    stderr="",
                    timed_out=False,
                )
                return
            self._active_runs += 1

        self._log.info(
            "cmd/run dispatched. request_id=%s command=%r timeout_s=%.1f cwd=%r",
            request_id,
            command,
            timeout_s,
            cwd,
        )
        self.publish_state()

        thread = threading.Thread(
            target=self._run_in_background,
            args=(request_id, command, timeout_s, cwd, env_overlay),
            daemon=True,
            name=f"exec-run-{request_id[:8] if request_id else 'anon'}",
        )
        thread.start()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run_in_background(
        self,
        request_id: str,
        command: str,
        timeout_s: float,
        cwd: str | None,
        env_overlay: dict[str, str] | None,
    ) -> None:
        """Execute the command and publish the result.  Runs in a daemon thread."""
        try:
            result = _executor.run(
                command,
                shell=True,
                timeout_s=timeout_s,
                cwd=cwd,
                env_overlay=env_overlay,
            )
        except Exception as exc:  # noqa: BLE001  # safety net — executor already catches most
            self._log.error(
                "Unexpected error in executor for request_id=%s: %s", request_id, exc
            )
            result = {
                "exit_code": None,
                "stdout": "",
                "stderr": "",
                "timed_out": False,
                "error": f"internal error: {exc}",
            }
        finally:
            with self._active_runs_lock:
                self._active_runs = max(0, self._active_runs - 1)

        with self._run_count_lock:
            self._run_count += 1
            run_count = self._run_count

        timed_out: bool = result.get("timed_out", False)
        exit_code: int | None = result.get("exit_code")
        stderr: str = result.get("stderr", "")
        stdout: str = result.get("stdout", "")
        exec_error: str | None = result.get("error")

        # ok=True even on non-zero exit codes — the command ran successfully.
        # Callers inspect exit_code to determine success at the application level.
        # ok=False only when the command could not be started or timed out.
        ok: bool = exec_error is None

        self._log.info(
            "cmd/run complete. request_id=%s exit_code=%s timed_out=%s ok=%s run_count=%d",
            request_id,
            exit_code,
            timed_out,
            ok,
            run_count,
        )

        self.publish_state()
        self.publish_telemetry("run_count", run_count)

        self._publish_run_result(
            request_id=request_id,
            ok=ok,
            error=exec_error,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            timed_out=timed_out,
        )

    def _publish_run_result(
        self,
        *,
        request_id: str,
        ok: bool,
        error: str | None,
        exit_code: int | None,
        stdout: str,
        stderr: str,
        timed_out: bool,
    ) -> None:
        """Publish to evt/run/result with the extended exec payload."""
        topic = self.context.topic("evt/run/result")
        payload = {
            "request_id": request_id,
            "ok": ok,
            "error": error,
            "exit_code": exit_code,
            "stdout": stdout,
            "stderr": stderr,
            "timed_out": timed_out,
        }
        try:
            body = json.dumps(payload)
        except (TypeError, ValueError) as exc:
            self._log.error("JSON encode failed for evt/run/result: %s", exc)
            return
        self.context.mqtt.publish(topic, body, qos=1, retain=False)

    # ------------------------------------------------------------------
    # Spawn / kill / list — managed long-running processes
    # ------------------------------------------------------------------

    def on_cmd_spawn(self, payload_str: str) -> None:
        """Start a detached long-running process and track it by name."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
        except json.JSONDecodeError:
            self._publish_simple_result("spawn", "", False, "invalid JSON in payload")
            return
        if not isinstance(payload, dict):
            self._publish_simple_result("spawn", "", False, "payload must be a JSON object")
            return

        request_id = str(payload.get("request_id", ""))
        name = payload.get("name")
        command = payload.get("command")

        if not isinstance(name, str) or not _SPAWN_NAME_RE.fullmatch(name):
            self._publish_simple_result(
                "spawn", request_id, False,
                "'name' must match ^[a-zA-Z0-9_-]+$",
            )
            return
        if not isinstance(command, str) or not command.strip():
            self._publish_simple_result("spawn", request_id, False, "'command' is required")
            return
        command = command.strip()

        if not _allowlist.is_allowed(command, self._allow_list):
            self._log.warning("cmd/spawn rejected by allow-list: name=%s", name)
            self._publish_simple_result(
                "spawn", request_id, False, "command rejected by allow-list",
            )
            return

        cwd = payload.get("cwd") or self._default_cwd
        env_overlay_raw = payload.get("env")
        env: dict[str, str] | None = None
        if isinstance(env_overlay_raw, dict):
            env = {**os.environ, **{str(k): str(v) for k, v in env_overlay_raw.items()}}

        with self._spawned_lock:
            existing = self._spawned.get(name)
            if existing is not None and existing["exit_code"] is None:
                self._publish_simple_result(
                    "spawn", request_id, False,
                    f"name '{name}' already spawned (pid {existing['pid']})",
                    extra={"name": name, "pid": existing["pid"]},
                )
                return
            # Count only alive processes toward the limit
            alive_count = sum(
                1 for e in self._spawned.values() if e["exit_code"] is None
            )
            if alive_count >= _MAX_SPAWNED_PROCESSES:
                self._publish_simple_result(
                    "spawn", request_id, False,
                    f"max spawned limit reached ({_MAX_SPAWNED_PROCESSES})",
                )
                return

            try:
                proc = subprocess.Popen(
                    ["bash", "-c", command],
                    cwd=cwd,
                    env=env,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True,  # pid == pgid; new process group survives parent death
                )
            except OSError as exc:
                self._publish_simple_result(
                    "spawn", request_id, False, f"failed to spawn: {exc}",
                )
                return

            entry: dict[str, Any] = {
                "proc": proc,
                "pid": proc.pid,
                "command": command,
                "cwd": cwd,
                "started_at": _utc_iso(),
                "exit_code": None,
                "killing": False,
            }
            self._spawned[name] = entry

        self._log.info("cmd/spawn ok: name=%s pid=%d command=%r", name, proc.pid, command)

        # Start monitor thread to detect exit
        monitor = threading.Thread(
            target=self._monitor_spawned,
            args=(name,),
            daemon=True,
            name=f"exec-monitor-{name}",
        )
        monitor.start()

        self.publish_state()
        self._publish_simple_result(
            "spawn", request_id, True, None,
            extra={"name": name, "pid": proc.pid},
        )

    def on_cmd_kill(self, payload_str: str) -> None:
        """Kill a previously spawned process by name (SIGTERM → SIGKILL). Non-blocking."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
        except json.JSONDecodeError:
            self._publish_simple_result("kill", "", False, "invalid JSON in payload")
            return
        if not isinstance(payload, dict):
            self._publish_simple_result("kill", "", False, "payload must be a JSON object")
            return

        request_id = str(payload.get("request_id", ""))
        name = payload.get("name")
        sigterm_timeout_s = payload.get("sigterm_timeout_s", _DEFAULT_SIGTERM_TIMEOUT_S)
        try:
            sigterm_timeout_s = max(0.0, float(sigterm_timeout_s))
        except (TypeError, ValueError):
            sigterm_timeout_s = _DEFAULT_SIGTERM_TIMEOUT_S

        if not isinstance(name, str) or not _SPAWN_NAME_RE.fullmatch(name):
            self._publish_simple_result(
                "kill", request_id, False,
                "'name' must match ^[a-zA-Z0-9_-]+$",
            )
            return

        with self._spawned_lock:
            entry = self._spawned.get(name)
            if entry is None:
                self._publish_simple_result(
                    "kill", request_id, False, f"unknown name '{name}'",
                )
                return
            if entry["exit_code"] is not None:
                self._publish_simple_result(
                    "kill", request_id, True, None,
                    extra={"name": name, "exit_code": entry["exit_code"], "already_dead": True},
                )
                return
            if entry.get("killing"):
                self._publish_simple_result(
                    "kill", request_id, False,
                    f"kill already in progress for '{name}'",
                )
                return
            entry["killing"] = True
            proc: subprocess.Popen = entry["proc"]
            pid = entry["pid"]

        # Dispatch the blocking wait to a background thread so the MQTT callback
        # thread is never held.
        thread = threading.Thread(
            target=self._kill_process_bg,
            args=(name, pid, proc, sigterm_timeout_s, request_id),
            daemon=True,
            name=f"exec-kill-{name}",
        )
        thread.start()

    def _kill_process_bg(
        self,
        name: str,
        pid: int,
        proc: subprocess.Popen,
        sigterm_timeout_s: float,
        request_id: str,
    ) -> None:
        """Background thread: SIGTERM → wait → SIGKILL a spawned process."""
        # Since start_new_session=True, pid == pgid — no need for os.getpgid().
        exit_code: int | None = None
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass  # already gone
        except OSError as exc:
            with self._spawned_lock:
                entry = self._spawned.get(name)
                if entry is not None:
                    entry["killing"] = False
            self._publish_simple_result(
                "kill", request_id, False, f"failed SIGTERM: {exc}",
                extra={"name": name, "pid": pid},
            )
            return

        try:
            exit_code = proc.wait(timeout=sigterm_timeout_s)
        except subprocess.TimeoutExpired:
            self._log.warning("name=%s did not exit after SIGTERM, sending SIGKILL", name)
            try:
                os.killpg(pid, signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
            try:
                exit_code = proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                self._log.error("name=%s did not exit even after SIGKILL", name)

        with self._spawned_lock:
            entry = self._spawned.get(name)
            if entry is not None:
                entry["exit_code"] = exit_code if exit_code is not None else -9
                entry["killing"] = False

        self._log.info("cmd/kill ok: name=%s pid=%d exit_code=%s", name, pid, exit_code)
        self.publish_state()
        self._publish_simple_result(
            "kill", request_id, True, None,
            extra={"name": name, "pid": pid, "exit_code": exit_code},
        )

    def on_cmd_list(self, payload_str: str) -> None:
        """List all managed spawned processes."""
        try:
            payload = json.loads(payload_str) if payload_str else {}
            request_id = str(payload.get("request_id", "")) if isinstance(payload, dict) else ""
        except json.JSONDecodeError:
            request_id = ""

        with self._spawned_lock:
            spawned = [
                {
                    "name": name,
                    "pid": entry["pid"],
                    "command": entry["command"],
                    "started_at": entry["started_at"],
                    "alive": entry["exit_code"] is None,
                    "exit_code": entry["exit_code"],
                }
                for name, entry in self._spawned.items()
            ]

        self._publish_simple_result(
            "list", request_id, True, None, extra={"spawned": spawned},
        )

    def _monitor_spawned(self, name: str) -> None:
        """Background: wait for a spawned process to exit, then update state."""
        with self._spawned_lock:
            entry = self._spawned.get(name)
            if entry is None:
                return
            proc = entry["proc"]

        try:
            rc = proc.wait()
        except Exception as exc:  # noqa: BLE001
            self._log.exception("monitor for name=%s failed: %s", name, exc)
            rc = -1

        with self._spawned_lock:
            entry = self._spawned.get(name)
            if entry is not None and entry["exit_code"] is None:
                entry["exit_code"] = rc
                self._log.info("spawned process exited: name=%s exit_code=%s", name, rc)

        try:
            self.publish_state()
        except Exception:  # noqa: BLE001
            pass

    def _publish_simple_result(
        self,
        action: str,
        request_id: str,
        ok: bool,
        error: str | None,
        *,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Publish to evt/{action}/result with a minimal payload."""
        topic = self.context.topic(f"evt/{action}/result")
        payload: dict[str, Any] = {
            "request_id": request_id,
            "ok": ok,
            "error": error,
        }
        if extra:
            payload.update(extra)
        try:
            body = json.dumps(payload)
        except (TypeError, ValueError) as exc:
            self._log.error("JSON encode failed for evt/%s/result: %s", action, exc)
            return
        self.context.mqtt.publish(topic, body, qos=1, retain=False)

    def _stop(self) -> None:
        """Kill all tracked spawned processes before stopping the component."""
        with self._spawned_lock:
            alive = [
                (name, entry) for name, entry in self._spawned.items()
                if entry["exit_code"] is None
            ]

        def _kill_one(name: str, entry: dict) -> None:
            pid = entry["pid"]
            proc: subprocess.Popen = entry["proc"]
            # Since start_new_session=True, pid == pgid.
            try:
                os.killpg(pid, signal.SIGTERM)
                self._log.info("_stop: SIGTERM sent to name=%s pid=%d", name, pid)
            except (ProcessLookupError, OSError):
                self._log.info("_stop: name=%s pid=%d already gone", name, pid)
                return

            try:
                exit_code = proc.wait(timeout=_DEFAULT_SIGTERM_TIMEOUT_S)
                self._log.info("_stop: name=%s pid=%d exited cleanly exit_code=%s", name, pid, exit_code)
            except subprocess.TimeoutExpired:
                self._log.warning("_stop: name=%s pid=%d did not exit after SIGTERM, sending SIGKILL", name, pid)
                try:
                    os.killpg(pid, signal.SIGKILL)
                except (ProcessLookupError, OSError):
                    pass
                try:
                    exit_code = proc.wait(timeout=5.0)
                    self._log.info("_stop: name=%s pid=%d killed exit_code=%s", name, pid, exit_code)
                except subprocess.TimeoutExpired:
                    self._log.error("_stop: name=%s pid=%d did not die after SIGKILL", name, pid)
                    exit_code = None

            with self._spawned_lock:
                if name in self._spawned:
                    self._spawned[name]["exit_code"] = (
                        exit_code if exit_code is not None else -9
                    )

        if alive:
            threads = [
                threading.Thread(target=_kill_one, args=(name, entry), daemon=True)
                for name, entry in alive
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=_DEFAULT_SIGTERM_TIMEOUT_S + 6.0)

        self._log.info("ExecComponent stopped. total_runs=%d alive_killed=%d", self._run_count, len(alive))

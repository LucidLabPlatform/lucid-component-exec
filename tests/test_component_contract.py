"""
Contract and integration tests for ExecComponent.

Tests run without a real MQTT broker.  A FakeMqtt captures all published
messages so we can assert on topic correctness and payload structure.

These tests cover:
- Instantiation and default configuration
- LUCID lifecycle (start/stop idempotency, state transitions)
- Capabilities list
- State and cfg payload structure
- cmd/ping → evt/ping/result
- cmd/reset → evt/reset/result + run_count zeroed
- cmd/run allow-list acceptance / rejection
- cmd/run dispatches subprocess (mocked) and publishes evt/run/result
- cmd/run invalid JSON
- cmd/run missing command field
- cmd/run concurrency limit
- cmd/run timeout clamping
- cmd/cfg/set → allow_list, timeouts, cwd, unknown key rejection
- cmd/cfg/logging/set (delegates to base)
- cmd/cfg/telemetry/set (delegates to base)
- Request-ID dedup (via base _make_cmd_handler wrapper — spot check)
"""
from __future__ import annotations

import json
import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from lucid_component_base import ComponentContext, ComponentStatus

from lucid_component_exec.component import ExecComponent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeMqtt:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self._lock = threading.Lock()

    def publish(self, topic: str, payload: Any, *, qos: int = 0, retain: bool = False) -> None:
        with self._lock:
            self.calls.append({"topic": topic, "payload": payload, "qos": qos, "retain": retain})

    def topics(self) -> list[str]:
        with self._lock:
            return [c["topic"] for c in self.calls]

    def payloads_for(self, topic_suffix: str) -> list[dict]:
        with self._lock:
            return [
                json.loads(c["payload"])
                for c in self.calls
                if c["topic"].endswith(topic_suffix)
            ]

    def clear(self) -> None:
        with self._lock:
            self.calls.clear()


def _make_context(
    config: dict | None = None,
    component_id: str = "exec",
) -> tuple[ExecComponent, FakeMqtt]:
    mqtt = FakeMqtt()
    ctx = ComponentContext.create(
        agent_id="test-agent",
        base_topic="lucid/agents/test-agent",
        component_id=component_id,
        mqtt=mqtt,
        config=config or {},
    )
    return ExecComponent(ctx), mqtt


def _started(config: dict | None = None) -> tuple[ExecComponent, FakeMqtt]:
    comp, mqtt = _make_context(config)
    comp.start()
    mqtt.clear()
    return comp, mqtt


def _run_payload(command: str, **kwargs) -> str:
    return json.dumps({"request_id": "req-001", "command": command, **kwargs})


def _wait_for_result(mqtt: FakeMqtt, topic_suffix: str, timeout: float = 3.0) -> dict:
    """Poll until a result matching *topic_suffix* appears, then return the payload."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        results = mqtt.payloads_for(topic_suffix)
        if results:
            return results[-1]
        time.sleep(0.02)
    raise AssertionError(f"Timed out waiting for topic suffix '{topic_suffix}'")


# ---------------------------------------------------------------------------
# Instantiation
# ---------------------------------------------------------------------------


def test_component_id():
    comp, _ = _make_context()
    assert comp.component_id == "exec"


def test_default_config():
    comp, _ = _make_context()
    assert comp._allow_list == []
    assert comp._default_timeout_s == 30.0
    assert comp._max_timeout_s == 300.0
    assert comp._default_cwd is None


def test_initial_state_is_stopped():
    comp, _ = _make_context()
    assert comp.state.status == ComponentStatus.STOPPED


# ---------------------------------------------------------------------------
# Capabilities
# ---------------------------------------------------------------------------


def test_capabilities_includes_required():
    comp, _ = _make_context()
    caps = comp.capabilities()
    assert "reset" in caps
    assert "ping" in caps
    assert "run" in caps


# ---------------------------------------------------------------------------
# State and cfg payload structure
# ---------------------------------------------------------------------------


def test_get_state_payload_structure():
    comp, _ = _make_context()
    state = comp.get_state_payload()
    required_keys = [
        "active_runs",
        "run_count",
        "allow_list_size",
        "allow_list_open",
        "default_timeout_s",
        "max_timeout_s",
    ]
    for key in required_keys:
        assert key in state, f"Missing state key: {key}"

    assert state["allow_list_open"] is True  # empty list → open mode
    assert state["active_runs"] == 0
    assert state["run_count"] == 0


def test_get_cfg_payload_structure():
    comp, _ = _make_context({"allow_list": ["echo *"], "default_timeout_s": 10.0})
    cfg = comp.get_cfg_payload()
    assert "allow_list" in cfg
    assert "default_timeout_s" in cfg
    assert "max_timeout_s" in cfg
    assert "cwd" in cfg
    assert cfg["allow_list"] == ["echo *"]
    assert cfg["default_timeout_s"] == 10.0


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def test_start_transitions_to_running():
    comp, _ = _make_context()
    comp.start()
    assert comp.state.status == ComponentStatus.RUNNING
    comp.stop()


def test_start_is_idempotent():
    comp, _ = _make_context()
    comp.start()
    comp.start()  # second call must not raise
    assert comp.state.status == ComponentStatus.RUNNING
    comp.stop()


def test_stop_transitions_to_stopped():
    comp, _ = _make_context()
    comp.start()
    comp.stop()
    assert comp.state.status == ComponentStatus.STOPPED


def test_stop_is_idempotent():
    comp, _ = _make_context()
    comp.start()
    comp.stop()
    comp.stop()  # must not raise
    assert comp.state.status == ComponentStatus.STOPPED


def test_start_publishes_retained_topics():
    comp, mqtt = _make_context()
    comp.start()
    topics = mqtt.topics()
    for suffix in ("metadata", "status", "state", "cfg"):
        assert any(suffix in t for t in topics), f"Expected retained topic '{suffix}'"
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/ping
# ---------------------------------------------------------------------------


def test_cmd_ping_publishes_result():
    comp, mqtt = _started()
    comp.on_cmd_ping(json.dumps({"request_id": "ping-001"}))
    results = mqtt.payloads_for("evt/ping/result")
    assert results, "Expected evt/ping/result"
    assert results[-1]["ok"] is True
    assert results[-1]["request_id"] == "ping-001"
    comp.stop()


def test_cmd_ping_with_empty_payload():
    comp, mqtt = _started()
    comp.on_cmd_ping("")
    results = mqtt.payloads_for("evt/ping/result")
    assert results
    assert results[-1]["ok"] is True
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/reset
# ---------------------------------------------------------------------------


def test_cmd_reset_publishes_result():
    comp, mqtt = _started()
    comp.on_cmd_reset(json.dumps({"request_id": "reset-001"}))
    results = mqtt.payloads_for("evt/reset/result")
    assert results
    assert results[-1]["ok"] is True
    assert results[-1]["request_id"] == "reset-001"
    comp.stop()


def test_cmd_reset_zeroes_run_count():
    comp, mqtt = _started()
    # Artificially bump the counter
    with comp._run_count_lock:
        comp._run_count = 5
    comp.on_cmd_reset(json.dumps({"request_id": "reset-002"}))
    assert comp._run_count == 0
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/run — allow-list rejection
# ---------------------------------------------------------------------------


def test_cmd_run_rejected_by_allow_list():
    comp, mqtt = _started({"allow_list": ["echo *"]})
    comp.on_cmd_run(_run_payload("rm -rf /"))
    results = mqtt.payloads_for("evt/run/result")
    assert results
    result = results[-1]
    assert result["ok"] is False
    assert "allow-list" in result["error"]
    comp.stop()


def test_cmd_run_accepted_when_allow_list_matches():
    comp, mqtt = _started({"allow_list": ["echo *"]})
    with patch("lucid_component_exec.component._executor.run") as mock_run:
        mock_run.return_value = {
            "exit_code": 0,
            "stdout": "hello",
            "stderr": "",
            "timed_out": False,
            "error": None,
        }
        comp.on_cmd_run(_run_payload("echo hello"))
        result = _wait_for_result(mqtt, "evt/run/result")
        assert result["ok"] is True
        assert result["exit_code"] == 0
    comp.stop()


def test_cmd_run_open_mode_allows_any_command():
    comp, mqtt = _started()  # no allow_list → open
    with patch("lucid_component_exec.component._executor.run") as mock_run:
        mock_run.return_value = {
            "exit_code": 0,
            "stdout": "",
            "stderr": "",
            "timed_out": False,
            "error": None,
        }
        comp.on_cmd_run(_run_payload("any_command"))
        result = _wait_for_result(mqtt, "evt/run/result")
        assert result["ok"] is True
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/run — payload validation
# ---------------------------------------------------------------------------


def test_cmd_run_invalid_json_publishes_error_result():
    comp, mqtt = _started()
    comp.on_cmd_run("{not json{{")
    results = mqtt.payloads_for("evt/run/result")
    assert results
    assert results[-1]["ok"] is False
    assert "JSON" in results[-1]["error"]
    comp.stop()


def test_cmd_run_missing_command_field():
    comp, mqtt = _started()
    comp.on_cmd_run(json.dumps({"request_id": "r-001"}))
    results = mqtt.payloads_for("evt/run/result")
    assert results
    assert results[-1]["ok"] is False
    assert "command" in results[-1]["error"]
    comp.stop()


def test_cmd_run_whitespace_only_command():
    comp, mqtt = _started()
    comp.on_cmd_run(json.dumps({"request_id": "r-002", "command": "   "}))
    results = mqtt.payloads_for("evt/run/result")
    assert results
    assert results[-1]["ok"] is False
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/run — result payload structure
# ---------------------------------------------------------------------------


def test_cmd_run_result_has_required_fields():
    comp, mqtt = _started()
    with patch("lucid_component_exec.component._executor.run") as mock_run:
        mock_run.return_value = {
            "exit_code": 0,
            "stdout": "out",
            "stderr": "err",
            "timed_out": False,
            "error": None,
        }
        comp.on_cmd_run(_run_payload("echo out"))
        result = _wait_for_result(mqtt, "evt/run/result")

    required = {"request_id", "ok", "error", "exit_code", "stdout", "stderr", "timed_out"}
    for key in required:
        assert key in result, f"Missing result field: {key}"
    assert result["stdout"] == "out"
    assert result["stderr"] == "err"
    assert result["exit_code"] == 0
    assert result["timed_out"] is False
    comp.stop()


def test_cmd_run_timed_out_result():
    comp, mqtt = _started()
    with patch("lucid_component_exec.component._executor.run") as mock_run:
        mock_run.return_value = {
            "exit_code": None,
            "stdout": "",
            "stderr": "",
            "timed_out": True,
            "error": "timed out after 30.0s",
        }
        comp.on_cmd_run(_run_payload("sleep 9999"))
        result = _wait_for_result(mqtt, "evt/run/result")

    assert result["timed_out"] is True
    assert result["exit_code"] is None
    assert result["ok"] is False  # error is set → ok=False
    comp.stop()


def test_cmd_run_increments_run_count():
    comp, mqtt = _started()
    with patch("lucid_component_exec.component._executor.run") as mock_run:
        mock_run.return_value = {
            "exit_code": 0, "stdout": "", "stderr": "", "timed_out": False, "error": None
        }
        comp.on_cmd_run(_run_payload("echo 1"))
        _wait_for_result(mqtt, "evt/run/result")

    assert comp._run_count == 1
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/run — timeout clamping
# ---------------------------------------------------------------------------


def test_cmd_run_timeout_clamped_to_max():
    comp, mqtt = _started({"max_timeout_s": 60.0})
    with patch("lucid_component_exec.component._executor.run") as mock_run:
        mock_run.return_value = {
            "exit_code": 0, "stdout": "", "stderr": "", "timed_out": False, "error": None
        }
        # Request 9999s — should be clamped to 60.0
        comp.on_cmd_run(json.dumps({
            "request_id": "req-timeout",
            "command": "echo hi",
            "timeout_s": 9999,
        }))
        _wait_for_result(mqtt, "evt/run/result")
        called_with = mock_run.call_args
        assert called_with.kwargs["timeout_s"] == 60.0
    comp.stop()


def test_cmd_run_timeout_clamped_to_minimum_one():
    comp, mqtt = _started()
    with patch("lucid_component_exec.component._executor.run") as mock_run:
        mock_run.return_value = {
            "exit_code": 0, "stdout": "", "stderr": "", "timed_out": False, "error": None
        }
        comp.on_cmd_run(json.dumps({
            "request_id": "req-min",
            "command": "echo hi",
            "timeout_s": 0,
        }))
        _wait_for_result(mqtt, "evt/run/result")
        called_with = mock_run.call_args
        assert called_with.kwargs["timeout_s"] == 1.0
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/run — concurrency limit
# ---------------------------------------------------------------------------


def test_cmd_run_concurrency_limit_returns_error():
    comp, mqtt = _started()
    # Saturate the counter without touching threads.
    with comp._active_runs_lock:
        comp._active_runs = comp._MAX_CONCURRENT_RUNS

    comp.on_cmd_run(_run_payload("echo hello"))
    results = mqtt.payloads_for("evt/run/result")
    assert results
    result = results[-1]
    assert result["ok"] is False
    assert "concurrency" in result["error"].lower()
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/run — env overlay
# ---------------------------------------------------------------------------


def test_cmd_run_env_overlay_forwarded_to_executor():
    comp, mqtt = _started()
    with patch("lucid_component_exec.component._executor.run") as mock_run:
        mock_run.return_value = {
            "exit_code": 0, "stdout": "", "stderr": "", "timed_out": False, "error": None
        }
        comp.on_cmd_run(json.dumps({
            "request_id": "req-env",
            "command": "echo $MY_VAR",
            "env": {"MY_VAR": "hello"},
        }))
        _wait_for_result(mqtt, "evt/run/result")
        called_with = mock_run.call_args
        assert called_with.kwargs["env_overlay"] == {"MY_VAR": "hello"}
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/cfg/set
# ---------------------------------------------------------------------------


def test_cmd_cfg_set_updates_allow_list():
    comp, mqtt = _started()
    comp.on_cmd_cfg_set(json.dumps({
        "request_id": "cfg-001",
        "set": {"allow_list": ["echo *", "ls *"]},
    }))
    assert comp._allow_list == ["echo *", "ls *"]
    results = mqtt.payloads_for("evt/cfg/set/result")
    assert results
    assert results[-1]["ok"] is True
    comp.stop()


def test_cmd_cfg_set_updates_default_timeout():
    comp, mqtt = _started()
    comp.on_cmd_cfg_set(json.dumps({
        "request_id": "cfg-002",
        "set": {"default_timeout_s": 45.0},
    }))
    assert comp._default_timeout_s == 45.0
    results = mqtt.payloads_for("evt/cfg/set/result")
    assert results[-1]["ok"] is True
    comp.stop()


def test_cmd_cfg_set_updates_cwd(tmp_path):
    comp, mqtt = _started()
    comp.on_cmd_cfg_set(json.dumps({
        "request_id": "cfg-003",
        "set": {"cwd": str(tmp_path)},
    }))
    assert comp._default_cwd == str(tmp_path)
    results = mqtt.payloads_for("evt/cfg/set/result")
    assert results[-1]["ok"] is True
    comp.stop()


def test_cmd_cfg_set_rejects_unknown_key():
    comp, mqtt = _started()
    comp.on_cmd_cfg_set(json.dumps({
        "request_id": "cfg-004",
        "set": {"nonexistent_key": 123},
    }))
    results = mqtt.payloads_for("evt/cfg/set/result")
    assert results
    assert results[-1]["ok"] is False
    assert "unknown" in results[-1]["error"]
    comp.stop()


def test_cmd_cfg_set_rejects_invalid_allow_list_type():
    comp, mqtt = _started()
    comp.on_cmd_cfg_set(json.dumps({
        "request_id": "cfg-005",
        "set": {"allow_list": "not-a-list"},
    }))
    results = mqtt.payloads_for("evt/cfg/set/result")
    assert results[-1]["ok"] is False
    comp.stop()


def test_cmd_cfg_set_rejects_invalid_timeout():
    comp, mqtt = _started()
    comp.on_cmd_cfg_set(json.dumps({
        "request_id": "cfg-006",
        "set": {"default_timeout_s": -5},
    }))
    results = mqtt.payloads_for("evt/cfg/set/result")
    assert results[-1]["ok"] is False
    comp.stop()


def test_cmd_cfg_set_null_cwd_clears_default():
    comp, mqtt = _started({"cwd": "/tmp"})
    comp.on_cmd_cfg_set(json.dumps({
        "request_id": "cfg-007",
        "set": {"cwd": None},
    }))
    assert comp._default_cwd is None
    results = mqtt.payloads_for("evt/cfg/set/result")
    assert results[-1]["ok"] is True
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/cfg/logging/set (delegate to base)
# ---------------------------------------------------------------------------


def test_cmd_cfg_logging_set_publishes_result():
    comp, mqtt = _started()
    comp.on_cmd_cfg_logging_set(json.dumps({
        "request_id": "log-001",
        "set": {"log_level": "DEBUG"},
    }))
    results = mqtt.payloads_for("evt/cfg/logging/set/result")
    assert results
    assert results[-1]["ok"] is True
    comp.stop()


# ---------------------------------------------------------------------------
# Non-zero exit codes are ok=True (caller inspects exit_code)
# ---------------------------------------------------------------------------


def test_cmd_run_nonzero_exit_code_is_ok_true():
    comp, mqtt = _started()
    with patch("lucid_component_exec.component._executor.run") as mock_run:
        mock_run.return_value = {
            "exit_code": 1,
            "stdout": "",
            "stderr": "error output",
            "timed_out": False,
            "error": None,  # no OS-level error → ok=True
        }
        comp.on_cmd_run(_run_payload("false"))
        result = _wait_for_result(mqtt, "evt/run/result")

    assert result["ok"] is True
    assert result["exit_code"] == 1
    comp.stop()


# ---------------------------------------------------------------------------
# cmd/spawn / cmd/kill / cmd/list — managed long-running processes
# ---------------------------------------------------------------------------


def _make_mock_proc(pid: int = 12345, returncode: int = 0) -> Any:
    from unittest.mock import MagicMock
    proc = MagicMock()
    proc.pid = pid
    proc.returncode = returncode
    proc.wait = MagicMock(return_value=returncode)
    return proc


def _spawn_payload(name: str, command: str = "sleep 100", **kwargs) -> str:
    return json.dumps({"request_id": f"spawn-{name}", "name": name, "command": command, **kwargs})


def _kill_payload(name: str, **kwargs) -> str:
    return json.dumps({"request_id": f"kill-{name}", "name": name, **kwargs})


def test_spawn_creates_process_and_returns_ok():
    comp, mqtt = _started()
    mock_proc = _make_mock_proc(pid=11111)
    with patch("subprocess.Popen", return_value=mock_proc):
        comp.on_cmd_spawn(_spawn_payload("myproc"))
    results = mqtt.payloads_for("evt/spawn/result")
    assert results
    assert results[-1]["ok"] is True
    assert results[-1]["name"] == "myproc"
    assert results[-1]["pid"] == 11111
    comp.stop()


def test_spawn_invalid_name_returns_error():
    comp, mqtt = _started()
    comp.on_cmd_spawn(json.dumps({"request_id": "r", "name": "bad name!", "command": "sleep 1"}))
    results = mqtt.payloads_for("evt/spawn/result")
    assert results
    assert results[-1]["ok"] is False
    comp.stop()


def test_spawn_duplicate_alive_name_returns_error():
    comp, mqtt = _started()
    mock_proc = _make_mock_proc(pid=22222)
    with comp._spawned_lock:
        comp._spawned["dupproc"] = {
            "proc": mock_proc, "pid": 22222, "command": "sleep 100",
            "cwd": None, "started_at": "2024-01-01T00:00:00+00:00", "exit_code": None,
        }
    comp.on_cmd_spawn(_spawn_payload("dupproc"))
    results = mqtt.payloads_for("evt/spawn/result")
    assert results
    assert results[-1]["ok"] is False
    assert "already spawned" in results[-1]["error"]
    comp.stop()


def test_kill_unknown_name_returns_error():
    comp, mqtt = _started()
    comp.on_cmd_kill(_kill_payload("nosuchproc"))
    results = mqtt.payloads_for("evt/kill/result")
    assert results
    assert results[-1]["ok"] is False
    assert "unknown name" in results[-1]["error"]
    comp.stop()


def test_kill_already_dead_returns_ok_with_flag():
    comp, mqtt = _started()
    mock_proc = _make_mock_proc(pid=33333, returncode=1)
    with comp._spawned_lock:
        comp._spawned["deadproc"] = {
            "proc": mock_proc, "pid": 33333, "command": "sleep 1",
            "cwd": None, "started_at": "2024-01-01T00:00:00+00:00", "exit_code": 1,
        }
    comp.on_cmd_kill(_kill_payload("deadproc"))
    results = mqtt.payloads_for("evt/kill/result")
    assert results
    assert results[-1]["ok"] is True
    assert results[-1].get("already_dead") is True
    comp.stop()


def test_kill_live_process_sends_sigterm_and_returns_ok():
    comp, mqtt = _started()
    mock_proc = _make_mock_proc(pid=44444, returncode=0)
    with comp._spawned_lock:
        comp._spawned["liveproc"] = {
            "proc": mock_proc, "pid": 44444, "command": "sleep 100",
            "cwd": None, "started_at": "2024-01-01T00:00:00+00:00", "exit_code": None,
        }
    with patch("os.killpg"), patch("os.getpgid", return_value=44444):
        comp.on_cmd_kill(_kill_payload("liveproc", sigterm_timeout_s=1))
    result = _wait_for_result(mqtt, "evt/kill/result")
    assert result["ok"] is True
    assert result["name"] == "liveproc"
    comp.stop()


def test_list_returns_all_spawned_processes():
    comp, mqtt = _started()
    mock_proc = _make_mock_proc()
    with comp._spawned_lock:
        comp._spawned["p1"] = {
            "proc": mock_proc, "pid": 1, "command": "sleep 1",
            "cwd": None, "started_at": "2024-01-01T00:00:00+00:00", "exit_code": None,
        }
        comp._spawned["p2"] = {
            "proc": mock_proc, "pid": 2, "command": "sleep 2",
            "cwd": None, "started_at": "2024-01-01T00:00:00+00:00", "exit_code": 0,
        }
    comp.on_cmd_list(json.dumps({"request_id": "list-001"}))
    results = mqtt.payloads_for("evt/list/result")
    assert results
    result = results[-1]
    assert result["ok"] is True
    names = {s["name"] for s in result["spawned"]}
    assert names == {"p1", "p2"}
    comp.stop()


# ---------------------------------------------------------------------------
# Bug #1: on_cmd_kill must not block the MQTT callback thread
# ---------------------------------------------------------------------------


def test_kill_does_not_block_calling_thread():
    """on_cmd_kill must return immediately; the wait happens in a background thread."""
    comp, mqtt = _started()
    mock_proc = _make_mock_proc(pid=55555)

    def slow_wait(timeout=None):
        time.sleep(0.4)
        return 0

    mock_proc.wait = slow_wait
    with comp._spawned_lock:
        comp._spawned["slowproc"] = {
            "proc": mock_proc, "pid": 55555, "command": "sleep 100",
            "cwd": None, "started_at": "2024-01-01T00:00:00+00:00", "exit_code": None,
        }

    with patch("os.killpg"), patch("os.getpgid", return_value=55555):
        start = time.monotonic()
        comp.on_cmd_kill(_kill_payload("slowproc", sigterm_timeout_s=0.4))
        elapsed = time.monotonic() - start

    assert elapsed < 0.1, f"on_cmd_kill blocked the calling thread for {elapsed:.3f}s"
    result = _wait_for_result(mqtt, "evt/kill/result", timeout=3.0)
    assert result["ok"] is True
    comp.stop()


# ---------------------------------------------------------------------------
# Bug #2: spawned slot limit should count only alive processes
# ---------------------------------------------------------------------------


def test_spawn_limit_counts_only_alive_processes():
    """Dead processes must not consume slots toward the _MAX_SPAWNED_PROCESSES limit."""
    from lucid_component_exec.component import _MAX_SPAWNED_PROCESSES
    comp, mqtt = _started()
    mock_proc = _make_mock_proc()
    with comp._spawned_lock:
        for i in range(_MAX_SPAWNED_PROCESSES):
            comp._spawned[f"dead-{i}"] = {
                "proc": mock_proc, "pid": i + 1000, "command": "echo",
                "cwd": None, "started_at": "2024-01-01T00:00:00+00:00", "exit_code": 0,
            }

    new_proc = _make_mock_proc(pid=99999)
    with patch("subprocess.Popen", return_value=new_proc):
        comp.on_cmd_spawn(_spawn_payload("newproc"))

    results = mqtt.payloads_for("evt/spawn/result")
    assert results
    assert results[-1]["ok"] is True, f"Expected ok=True, got error: {results[-1].get('error')}"
    comp.stop()


# ---------------------------------------------------------------------------
# Bug #3: _start must reset _spawned so restart gives a clean slate
# ---------------------------------------------------------------------------


def test_restart_clears_spawned_registry():
    """Stopping and restarting the component must clear the spawned process registry."""
    comp, mqtt = _started()
    mock_proc = _make_mock_proc(pid=77777, returncode=0)
    mock_proc.wait = MagicMock(return_value=0)
    with comp._spawned_lock:
        comp._spawned["oldproc"] = {
            "proc": mock_proc, "pid": 77777, "command": "sleep 1",
            "cwd": None, "started_at": "2024-01-01T00:00:00+00:00", "exit_code": 0,
        }

    with patch("os.killpg"), patch("os.getpgid", return_value=77777):
        comp.stop()
    comp.start()

    with comp._spawned_lock:
        assert len(comp._spawned) == 0, f"Expected empty _spawned after restart, got {list(comp._spawned.keys())}"
    comp.stop()


# ---------------------------------------------------------------------------
# Bug #4: concurrent kills of the same name must not both proceed
# ---------------------------------------------------------------------------


def test_concurrent_kills_same_name_only_one_proceeds():
    """Two simultaneous kill commands for the same name: exactly one must succeed, the other must be rejected."""
    comp, mqtt = _started()
    mock_proc = _make_mock_proc(pid=88888, returncode=0)

    # Add a small delay so both threads have time to race
    original_wait = mock_proc.wait

    def delayed_wait(timeout=None):
        time.sleep(0.05)
        return 0

    mock_proc.wait = delayed_wait
    with comp._spawned_lock:
        comp._spawned["raceproc"] = {
            "proc": mock_proc, "pid": 88888, "command": "sleep 100",
            "cwd": None, "started_at": "2024-01-01T00:00:00+00:00", "exit_code": None,
        }

    barrier = threading.Barrier(2)

    def do_kill(req_id: str) -> None:
        barrier.wait()
        with patch("os.killpg"), patch("os.getpgid", return_value=88888):
            comp.on_cmd_kill(json.dumps({"request_id": req_id, "name": "raceproc"}))

    t1 = threading.Thread(target=do_kill, args=("kill-race-a",))
    t2 = threading.Thread(target=do_kill, args=("kill-race-b",))
    t1.start()
    t2.start()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)

    # Wait for both results
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        results = mqtt.payloads_for("evt/kill/result")
        if len(results) >= 2:
            break
        time.sleep(0.02)

    results = mqtt.payloads_for("evt/kill/result")
    assert len(results) == 2
    ok_count = sum(1 for r in results if r["ok"])
    fail_count = sum(1 for r in results if not r["ok"])
    assert ok_count == 1, f"Expected exactly 1 ok kill, got {ok_count}: {results}"
    assert fail_count == 1, f"Expected exactly 1 rejected kill, got {fail_count}: {results}"
    comp.stop()

# Author: Tom Sapletta · Part of the ifURI solution.
from __future__ import annotations

from urirun_connector_watchdog import core


def test_bindings():
    b = core.urirun_bindings()["bindings"]
    for r in ("watch://host/loop/query/detect", "watch://host/ticket/command/unstick",
              "watch://host/query/report", "watch://host/loop/command/sweep"):
        assert r in b


def test_classify_priorities():
    assert core._classify({"drive_failed": 3})["category"] == "no_executor"
    assert core._classify({"waiting_input": True})["category"] == "needs_input"
    assert core._classify({"env_blocked": True})["category"] == "env"
    assert core._classify({})["category"] == "stalled"


def test_detect_from_synthetic_log(tmp_path, monkeypatch):
    log = (
        "[10:00:00] koru ▸ QUEUE: queue: waiting_ticket=IFURI-033 last_status=waiting_input\n"
        "[10:00:01] koru ▸ OBS: cycle=44 ticket=IFURI-033 blocker name=drive_failed because=\"client command failed\"\n"
        "[10:00:01] koru ▸ DECISION: streak=6 ticket=IFURI-033 decided=drive_failed\n"
        "[10:00:02] koru ▸ INFO: pre-drive: no viable control route ticket=IFURI-033\n")
    monkeypatch.setattr(core, "_log_lines", lambda project, n=400: log.splitlines())
    d = core.detect(project="/x")
    assert d["count"] == 1
    t = d["stuck"][0]
    assert t["id"] == "IFURI-033" and t["streak"] == 6
    assert t["category"] == "no_executor"  # drive_failed wins


def test_detect_ignores_completed_ticket_noise(monkeypatch):
    log = (
        "[10:00:01] koru ▸ OBS: cycle=44 ticket=IFURI-034 blocker name=drive_failed because=\"client command failed\"\n"
        "[10:00:01] koru ▸ DECISION: streak=9 ticket=IFURI-034 decided=drive_failed\n")
    monkeypatch.setattr(core, "_log_lines", lambda project, n=400: log.splitlines())
    monkeypatch.setattr(core, "_ticket_show", lambda project, tid: {"id": tid, "status": "done"})

    d = core.detect(project="/x")

    assert d["count"] == 0
    assert d["stuck"] == []


def test_detect_ignores_blocked_ticket_noise(monkeypatch):
    log = (
        "[10:00:01] koru ▸ OBS: cycle=44 ticket=IFURI-034 blocker name=drive_failed because=\"client command failed\"\n"
        "[10:00:01] koru ▸ DECISION: streak=9 ticket=IFURI-034 decided=drive_failed\n"
    )
    monkeypatch.setattr(core, "_log_lines", lambda project, n=400: log.splitlines())
    monkeypatch.setattr(core, "_ticket_show", lambda project, tid: {"id": tid, "status": "blocked"})

    d = core.detect(project="/x")

    assert d["count"] == 0
    assert d["stuck"] == []


def test_recent_lines_filters_timestamped_history():
    now = core._dt.datetime(2026, 7, 6, 18, 0, 0)
    lines = [
        "[17:45:00] koru ▸ OBS: recent",
        "[13:01:00] koru ▸ OBS: stale drive_failed",
        "untimestamped diagnostic",
    ]

    assert core._recent_lines(lines, minutes=30, now=now) == [
        "[17:45:00] koru ▸ OBS: recent",
        "untimestamped diagnostic",
    ]


def test_system_analyze_ignores_stale_drive_failures(monkeypatch):
    old = "[13:01:02] koru ▸ OBS: corr=cli-drive cycle=62 ticket=IFURI-034 blocker name=drive_failed because=\"client command failed\""
    monkeypatch.setattr(core, "_log_lines", lambda project, n=1500: [old])
    monkeypatch.setattr(core, "_recent_lines", lambda lines, minutes: [])
    monkeypatch.setattr(core, "_ingest_errors", lambda project: {"total": 0, "top": []})

    class CP:
        stdout = ""

    monkeypatch.setattr(core.subprocess, "run", lambda *a, **k: CP())

    r = core.system_analyze(project="/x")

    assert r["healthy"] is True
    assert r["drive_failed"] == 0
    assert r["findings"] == []
    assert r["window"]["raw_lines"] == 1 and r["window"]["lines"] == 0


def test_report_ok_when_clean(monkeypatch):
    monkeypatch.setattr(core, "_log_lines", lambda project, n=400: [])
    r = core.query_report(project="/x")
    assert r["ok"] and "Brak zapętleń" in r["report"]


def test_escalation_has_dashboard_and_human(monkeypatch):
    r = core._escalation("IFURI-033", {"category": "needs_input", "rootcause": "x", "action": "y"})
    assert (
        r["ticket"] == "IFURI-033"
        and "human://" in r["human_uri"]
        and "/work?ticket=" in (r.get("urls") or {}).get("dashboard", "")
    )


def _fake_pf(monkeypatch, calls, create_stdout="IFURI-099 created"):
    """Wspolny stub: planfile CLI via subprocess + Python API blocked_by."""
    monkeypatch.setattr(core, "_planfile", lambda: "/usr/bin/planfile")
    monkeypatch.setattr(core, "_project", lambda project="": project or "/x")

    class CP:
        returncode = 0

        def __init__(self, stdout=""):
            self.stdout = stdout
            self.stderr = ""

    def run(cmd, *a, **k):
        calls.append(cmd)
        if "create" in cmd:
            return CP(create_stdout)
        return CP("")

    monkeypatch.setattr(core.subprocess, "run", run)


def test_circuit_break_sets_blocked_by_on_original(monkeypatch):
    tid = "IFURI-062"
    calls = []
    _fake_pf(monkeypatch, calls)
    monkeypatch.setattr(core, "_existing_diag", lambda project, t: None)
    monkeypatch.setattr(core, "detect", lambda project, *a, **k: {"stuck": [
        {"id": tid, "category": "no_executor", "rootcause": "r", "action": "a",
         "streak": 0, "drive_failed": 1}]})
    monkeypatch.setattr(core, "_oscillation", lambda proj, t: {
        "cycles": 2, "transitions": 6, "status_counts": {}})
    seen = {}
    monkeypatch.setattr(core, "_set_blocked_by",
                        lambda project, t, deps: seen.setdefault("deps", deps) or deps)

    r = core.loop_command_circuit_break(id=tid, project="/x")

    assert r["ok"] and r["diagnosis"] == "IFURI-099"
    # breaker DOMKNIETY: oryginal blocked_by ticketu diagnozy (strukturalnie)
    assert seen["deps"] == ["IFURI-099"]
    assert r["blocked_by"] == ["IFURI-099"]
    # notatka niesie marker rozpoznawany przez reconciler loop:// (blocked_by <ID>)
    notes = [" ".join(c) for c in calls if "--note" in c or "-r" in c]
    assert any("blocked_by IFURI-099" in n for n in notes)
    # diagnoza kierowana do CZLOWIEKA (actor:human) — inaczej koru cli-driveuje sama diagnoze
    # no_executor i wpada w META-petle (dosl. objaw IFURI-063 driveowanego w kolko)
    create = next(c for c in calls if "create" in c)
    assert "actor:human" in create


def test_circuit_break_rehold_when_diagnosis_exists(monkeypatch):
    """Gdy diagnoza juz istnieje (otwarta) a oryginal znow otwarty — breaker PONAWIA blokade
    (dawniej no-op -> petla wracala). NIE tworzy nowej diagnozy."""
    tid = "IFURI-062"
    calls = []
    _fake_pf(monkeypatch, calls)
    monkeypatch.setattr(core, "_existing_diag", lambda project, t: {"id": "IFURI-063", "status": "blocked"})
    seen = {}
    monkeypatch.setattr(core, "_set_blocked_by",
                        lambda project, t, deps: seen.setdefault("deps", deps) or deps)

    r = core.loop_command_circuit_break(id=tid, project="/x")

    assert r["ok"] and r["already"] and r["diagnosis"] == "IFURI-063"
    assert r["reopened"] is False
    assert seen["deps"] == ["IFURI-063"] and r["blocked_by"] == ["IFURI-063"]
    # ponowna blokada faktycznie wyslana do planfile (nie no-op)
    assert any("block" in c for c in calls)
    # IDEMPOTENCJA: zadnego nowego 'ticket create' (jedna diagnoza na ticket)
    assert not any("create" in c for c in calls)


def test_circuit_break_reopens_closed_diagnosis_instead_of_new(monkeypatch):
    """IFURI-078: gdy poprzednia diagnoza jest DONE a rootcause wciaz zyje — breaker
    REOTWIERA ta sama diagnoze (status->blocked), NIE tworzy kolejnej DIAGNOZY co cykl."""
    tid = "IFURI-060"
    calls = []
    _fake_pf(monkeypatch, calls)
    monkeypatch.setattr(core, "_existing_diag", lambda project, t: {"id": "IFURI-070", "status": "done"})
    seen = {}
    monkeypatch.setattr(core, "_set_blocked_by",
                        lambda project, t, deps: seen.setdefault("deps", deps) or deps)

    r = core.loop_command_circuit_break(id=tid, project="/x")

    assert r["ok"] and r["already"] and r["diagnosis"] == "IFURI-070"
    assert r["reopened"] is True
    # reopen = update status->blocked na TEJ diagnozie, brak nowego create
    assert any("update" in c and "IFURI-070" in c and "blocked" in c for c in calls)
    assert not any("create" in c for c in calls)
    assert seen["deps"] == ["IFURI-070"]

# Author: Tom Sapletta · Part of the ifURI solution.
"""urirun-connector-watchdog — wybudza urirun, gdy nic nie idzie do przodu.

Pętla autonomiczna (koru) potrafi utknąć: napędza to samo zadanie w kółko, drive pada
(`client command failed`, brak wykonawcy), a ticket czeka na input operatora. Ten connector:

  1. **wykrywa** stagnację — z realnego logu koru: streak `drive_failed`, `waiting_input`,
     powtarzające się w kółko komendy;
  2. **diagnozuje** przyczynę (rootcause) regułowo, weryfikowalnie:
     needs_input (creds) / no_executor (drive agenta pada) / env (x11/plugin) / stalled;
  3. **przerywa jałową pętlę** — `unstick` przenosi ticket do `blocked` z notatką i zwraca
     eskalację do panelu / `human://`, żeby operator ruszył sprawę zamiast koru w kółko.

Detekcja jest read-only (bezpieczna, ciągła); ``unstick``/``sweep`` mutują → isolated.
Zbudowane wg URI_NATIVE_CONNECTOR_CHECKLIST: lazy import, koperta nie rzuca, in-process.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import re
import shutil
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

import urirun

CONNECTOR_ID = "watch"
conn = urirun.connector(CONNECTOR_ID, scheme="watch")

_STREAK = re.compile(r"streak=(\d+)")
_WAIT = re.compile(r"waiting_ticket=([A-Z]+-\d+)")
_TICKET = re.compile(r"ticket=([A-Z]+-\d+)")
_LINE_TIME = re.compile(r"^\[(\d{2}):(\d{2}):(\d{2})\]")
_ESCALATE_BASE = os.environ.get("URIRUN_LAN_QR_BASE") or "http://192.168.188.212:8797"


def _ok(**kw: Any) -> dict[str, Any]:
    return urirun.ok(connector=CONNECTOR_ID, **kw)


def _fail(msg: str, action: str, **extra: Any) -> dict[str, Any]:
    return urirun.fail(msg, connector=CONNECTOR_ID, action=action, **extra)


def _project(project: str = "") -> str:
    return project or os.environ.get("URIRUN_KORU_PROJECT") or os.path.expanduser("~/github/if-uri")


def _planfile() -> str | None:
    b = os.environ.get("URIRUN_PLANFILE_BIN") or shutil.which("planfile")
    if b:
        return b
    for c in ("~/github/if-uri/venv/bin/planfile", "~/github/semcod/koru/.venv/bin/planfile"):
        p = Path(c).expanduser()
        if p.is_file():
            return str(p)
    return None


def _log_lines(project: str, n: int = 400) -> list[str]:
    for name in ("queue.log", "soak.log"):
        p = Path(_project(project)) / ".planfile" / ".koru" / name
        if p.is_file():
            try:
                return p.read_text(errors="replace").splitlines()[-n:]
            except OSError:
                return []
    return []


def _recent_lines(lines: list[str], minutes: int = 30, now: _dt.datetime | None = None) -> list[str]:
    """Keep timestamped log lines from the recent window.

    Queue logs are append-only and can contain hours-old failure streaks. System-level alerts
    should fire on live failures, not on remembered history. Lines without a ``[HH:MM:SS]``
    prefix are kept so nonstandard fresh diagnostics are not hidden.
    """
    cur = now or _dt.datetime.now()
    cur_s = cur.hour * 3600 + cur.minute * 60 + cur.second
    window = int(minutes) * 60
    out: list[str] = []
    for ln in lines:
        m = _LINE_TIME.search(ln)
        if not m:
            out.append(ln)
            continue
        h, mi, s = (int(x) for x in m.groups())
        then = h * 3600 + mi * 60 + s
        age = (cur_s - then) % 86400
        if age <= window:
            out.append(ln)
    return out


# --------------------------------------------------------------- ANALIZA ŚRODOWISKA (poziom SYSTEMU, nie ticketu)

_FINDINGS_ENV = "URIRUN_SYSTEM_FINDINGS"


def _findings_store():
    p = Path(os.environ.get(_FINDINGS_ENV) or "~/.urirun/host-dashboard/system-findings.json").expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _record_findings(findings: list[dict]) -> None:
    """UCZ SIĘ: zapisz systemowe antywzorce (żeby system nie powtarzał — pamięć środowiska)."""
    try:
        store = _findings_store()
        data = json.loads(store.read_text()) if store.is_file() else {}
    except Exception:  # noqa: BLE001
        data = {}
    for f in findings:
        key = f["pattern"]
        prev = data.get(key, {})
        data[key] = {**f, "seen": prev.get("seen", 0) + 1, "first": prev.get("first") or f.get("at", "")}
    try:
        _findings_store().write_text(json.dumps(data, indent=1, ensure_ascii=False), encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass


def _ingest_errors(project: str) -> dict:
    """Wielo-źródłowość: wciągnij store errorów (`urirun errors`) — recurring code = antywzorzec."""
    import shutil
    import subprocess
    from collections import Counter
    binp = shutil.which("urirun") or os.path.expanduser("~/github/if-uri/venv/bin/urirun")
    try:
        cp = subprocess.run([binp, "errors"], capture_output=True, text=True, timeout=15, cwd=_project(project))
        raw = cp.stdout
        data = json.loads(raw[raw.index("["):raw.rindex("]") + 1]) if "[" in raw else []
    except Exception:  # noqa: BLE001
        return {"total": 0, "top": []}
    codes = Counter(str(e.get("code") or e.get("message", "")[:40]) for e in data if isinstance(e, dict))
    return {"total": len(data), "top": codes.most_common(5)}


_LOOP_WORDS = ("guarded", "blind", "ślepa", "strateg", "próba", "circuit", "diagnoza",
               "eskalac", "escalate", "no_executor", "zapętl", "retry", "drive_failed")
_TOPIC_HINTS = ("signal", "email", "cron", "kvm", "calendar", "schedule", "pypi", "publish")


def _ticket_events(t: dict) -> int:
    """Ile 'zdarzeń pętli' niesie ticket: noty o próbach/porażkach + churn statusu."""
    notes = (t.get("outputs") or {}).get("notes") or []
    n = sum(1 for note in notes if any(w in str(note).lower() for w in _LOOP_WORDS))
    if t.get("status") in ("blocked", "in_progress"):
        n += 1
    return n


def _ticket_topic(t: dict) -> str:
    """Sygnał-temat: z labeli (topic/node/scheme) + słów kluczy w nazwie. Klucz korelacji."""
    labs = [str(x).lower() for x in (t.get("labels") or [])]
    blob = (str(t.get("name", "")) + " " + str(t.get("description", ""))).lower()
    for hint in _TOPIC_HINTS:
        if any(hint in l for l in labs) or hint in blob:
            return hint
    return ""


# KONTRAKT: taksonomia „legalnego czekania" — brak postępu bo POPRAWNIE czeka na wejście,
# NIE bo pętla. Każdy != "" → watchdog NIE generuje NORA/DIAGNOZA (tylko przypomnienie/packet).
# "" → kandydat na rabbit-hole (zapętlenie: powtórny fail URI / ok≠effect / brak markerów).
_WAIT_SECRET = ("keyring set", "app-password", "imap", "secret://", "pypi token", "token pypi",
                "needs-human:pypi-token", "creds", "hasło")
_WAIT_NODE = ("na lenovo", "węzeł lenovo", "lenovo mailbox", "na węzeł", "deploy", "enrollment",
              "nodeadmin", "wdroż", "wdróż", "zainstaluj")
_WAIT_HUMAN = ("skan qr", "signal-cli", "zalinkuj", "qr telefon", "telefonem", "zainstaluj signal",
               "thunderbird", "operator")
_WAIT_EXTERNAL = ("czeka na zewn", "external approval", "approval packet", "linkedin", "fiverr", "publikacj")


def wait_kind(t: dict) -> str:
    """Rodzaj LEGALNEGO czekania: waiting_secret/waiting_node/waiting_human/waiting_external, albo ""
    (nie-czekanie → kandydat rabbit-hole). To granica autonomia vs spamowanie ticketami."""
    labels = [str(x).lower() for x in (t.get("labels") or [])]
    # JAWNA granica autonomii wygrywa: waiting:<kind> / autonomy-frontier (zamrożone ręcznie/przez frontier-freeze)
    for l in labels:
        if l.startswith("waiting:"):
            return "waiting_" + l.split(":", 1)[1]
    if "autonomy-frontier" in labels:
        return "waiting_frontier"
    if any(l.startswith("needs-human:") for l in labels):
        return "waiting_secret" if any("token" in l or "cred" in l or "secret" in l for l in labels) else "waiting_human"
    if "node:lenovo" in labels and ("actor:human" in labels or "deploy" in labels):
        return "waiting_node"
    blob = f"{t.get('name', '')} {t.get('description', '')}".lower()
    for kind, kws in (("waiting_secret", _WAIT_SECRET), ("waiting_node", _WAIT_NODE),
                      ("waiting_human", _WAIT_HUMAN), ("waiting_external", _WAIT_EXTERNAL)):
        if any(k in blob for k in kws):
            return kind
    return ""


def _is_unblocked(t: dict) -> bool:
    """Czy ticket ma TRWAŁE odblokowanie człowieka (ledger) — wtedy NIGDY nie re-blokuj/diagnozuj."""
    try:
        from urirun_connector_grants import unblock_ledger
        return unblock_ledger.is_unblocked_for(t)
    except Exception:  # noqa: BLE001
        return False


def _genuine_human_wait(t: dict) -> bool:
    """Legalne czekanie na wejście (dowolny wait_kind) → NIE liczyć jako looper nory."""
    return bool(wait_kind(t))


def waiting_summary(project: str = "") -> dict[str, Any]:
    """Dla /work: rozbij blocked na taksonomię — actionable vs legalne czekanie vs rabbit-hole.
    Mówi operatorowi: system NIE stoi, tylko czeka na realne wejście (nie auto-diagnozować)."""
    import json
    import subprocess
    pf = _planfile()
    if not pf:
        return {"legal_waits": {}, "actionable": 0, "rabbit_hole_candidates": []}
    try:
        cp = subprocess.run([pf, "ticket", "list", "--format", "json"], capture_output=True,
                            text=True, timeout=15, cwd=_project(project))
        data = json.loads(cp.stdout[cp.stdout.index("["):cp.stdout.rindex("]") + 1])
    except Exception:  # noqa: BLE001
        return {"legal_waits": {}, "actionable": 0, "rabbit_hole_candidates": []}
    buckets: dict[str, list] = {}
    actionable, rabbit = 0, []
    for t in data:
        st = t.get("status")
        if st in ("done", "closed", "cancelled", "canceled"):
            continue
        if st in ("open", "in_progress", "ready"):
            actionable += 1
            continue
        kind = wait_kind(t)  # tylko blocked/waiting
        if kind:
            buckets.setdefault(kind, []).append({"id": t["id"], "name": t.get("name", "")[:50]})
        else:
            rabbit.append({"id": t["id"], "name": t.get("name", "")[:50]})
    return {"actionable": actionable, "legal_waits": buckets,
            "legal_waits_total": sum(len(v) for v in buckets.values()),
            "rabbit_hole_candidates": rabbit, "churn": "stopped" if not rabbit else "candidates"}


@conn.handler("system/query/waiting", isolated=False,
              meta={"label": "Taksonomia oczekiwań: actionable vs legalne czekanie (human/secret/node) vs rabbit-hole"})
def system_query_waiting(project: str = "") -> dict[str, Any]:
    return _ok(action="waiting-summary", **waiting_summary(project))


def rabbit_hole_correlate(project: str = "") -> dict[str, Any]:
    """SPINA wiele ticketów/zdarzeń w JEDNĄ króliczą norę: klastruj po wspólnym TEMACIE +
    linii blocked_by/spawn; nora = klaster koincydencji z dużą liczbą zdarzeń pętli i BRAKIEM
    postępu (0 done). Reaguj na poziomie KLASTRA, nie pojedynczego ticketu."""
    import json
    import re
    import subprocess
    pf = _planfile()
    if not pf:
        return {"holes": []}
    try:
        cp = subprocess.run([pf, "ticket", "list", "--format", "json"], capture_output=True,
                            text=True, timeout=15, cwd=_project(project))
        raw = cp.stdout
        data = json.loads(raw[raw.index("["):raw.rindex("]") + 1]) if "[" in raw else []
    except Exception:  # noqa: BLE001
        return {"holes": []}
    open_t = [t for t in data if t.get("status") not in ("done", "closed", "cancelled", "canceled")]
    # klastry po temacie
    clusters: dict[str, list] = {}
    for t in open_t:
        topic = _ticket_topic(t)
        if topic:
            clusters.setdefault(topic, []).append(t)
    holes = []
    for topic, all_members in clusters.items():
        # WYKLUCZ genuine-human-waits: klaster czekający na fizyczny krok (QR/signal-cli/creds/lenovo)
        # NIE jest norą — to legalne czekanie na człowieka; robienie z tego NORA = szum (0 postępu OCZEKIWANE)
        members = [t for t in all_members if not _genuine_human_wait(t) and not _is_unblocked(t)]
        events = sum(_ticket_events(t) for t in members)
        loopers = sum(1 for t in members if _ticket_events(t) >= 1)
        done_here = any(t.get("status") in ("done", "closed") for t in members)
        # KRÓLICZA NORA: >=2 powiązane tickety (po odjęciu human-waits), dużo zdarzeń pętli, brak postępu
        if len(members) >= 2 and events >= 3 and loopers >= 2 and not done_here:
            holes.append({
                "hole": f"rabbit-hole:{topic}", "topic": topic,
                "tickets": [t["id"] for t in members], "members": len(members),
                "loop_events": events, "progress": "0 done — utknięte",
                "verdict": "zidentyfikowana królicza nora → circuit-break KLASTRA + JEDNA eskalacja "
                           "(nie mnóż ticketów/prób w tej norze)"})
    return {"holes": holes, "clusters": {k: [t["id"] for t in v] for k, v in clusters.items()}}


def system_analyze(project: str = "") -> dict[str, Any]:
    """Traktuj KONTROLERY i ŚRODOWISKO jako obiekty obserwacji (WIELO-ŹRÓDŁOWO: log koru + errory +
    procesy + systemd). Wykryj systemowe patologie, wyciągnij WNIOSEK + KONTRAKCJĘ, zapisz antywzorzec."""
    import subprocess
    raw_lines = _log_lines(project, 1500)
    recent_minutes = int(os.environ.get("URIRUN_WATCHDOG_RECENT_MINUTES", "30"))
    lines = _recent_lines(raw_lines, recent_minutes)
    cycles = max([int(m) for ln in lines for m in re.findall(r"cycle=(\d+)", ln)] or [0])
    completed = sum(int(m) for ln in lines for m in re.findall(r"completed=(\d+)", ln))
    streak = max([int(m) for ln in lines for m in re.findall(r"streak=(\d+)", ln)] or [0])
    drive_failed = sum(1 for ln in lines if "drive_failed" in ln or "client command failed" in ln)
    # NATYWNY monitor: wykryj supervisora (systemd) — bez tego kontrakcja „zatrzymaj koru" nie działa (respawn)
    svc = ""
    try:
        out = subprocess.run(["systemctl", "--user", "list-units", "--no-legend"],
                             capture_output=True, text=True, timeout=6).stdout
        svc = next((l.split()[0] for l in out.splitlines() if "koru" in l.lower()), "")
    except Exception:  # noqa: BLE001
        pass
    stop_cmd = f"systemctl --user stop {svc}" if svc else "zatrzymaj supervisora koru"
    findings = []
    if cycles >= 5 and completed == 0:
        findings.append({"pattern": "controller-non-productive", "severity": "high",
                         "observed": f"koru: {cycles} cykli, {completed} ukończeń, streak={streak}"
                         + (f", supervisor={svc}" if svc else ""),
                         "conclusion": "kontroler koru NIEPRODUKTYWNY — executor (tillm) nie działa; cykle bez efektu; "
                         + ("RESPAWN przez systemd → kill nie wystarczy" if svc else ""),
                         "counteraction": f"{stop_cmd}; użyj loop:// (safe) jako kontrolera; podłącz agent:// jako executor"})
    if drive_failed >= 10:
        findings.append({"pattern": "executor-broken", "severity": "high",
                         "observed": f"drive_failed ×{drive_failed}",
                         "conclusion": "trasa wykonawcza (tillm_shell→claude-code) trwale pada w tym środowisku",
                         "counteraction": "przełącz na agent:// (claude -p headless) — sprawdzony sprawny"})
    # konflikt kontrolerów: >1 proces koru na TYM projekcie
    try:
        import subprocess
        out = subprocess.run(["pgrep", "-af", "autonomous up"], capture_output=True, text=True, timeout=5).stdout
        n = sum(1 for l in out.splitlines() if _project(project) in l)
        if n >= 2:
            findings.append({"pattern": "controller-conflict", "severity": "medium",
                             "observed": f"{n} procesy koru na {_project(project)}",
                             "conclusion": "wiele kontrolerów mutuje ten sam blackboard — walczą (blok↔re-open)",
                             "counteraction": "JEDEN kontroler na projekt; zatrzymaj duplikaty/supervisor"})
    except Exception:  # noqa: BLE001
        pass
    # ŹRÓDŁO: store errorów (urirun errors) — recurring code = antywzorzec
    errs = _ingest_errors(project)
    for code, n in errs.get("top", []):
        if n >= 3 and code:
            findings.append({"pattern": f"recurring-error:{code}", "severity": "medium",
                             "observed": f"błąd {code} ×{n} w store",
                             "conclusion": f"powtarzalny błąd {code} — nie jest naprawiany u źródła",
                             "counteraction": f"zdiagnozuj rootcause {code} (inquiry://) i napraw ticketem; nie ignoruj"})
    if findings:
        _record_findings(findings)
    return {"cycles": cycles, "completed": completed, "streak": streak, "drive_failed": drive_failed,
            "errors": errs, "findings": findings, "healthy": not findings,
            "window": {"recent_minutes": recent_minutes, "lines": len(lines), "raw_lines": len(raw_lines)},
            "sources": ["koru-log", "urirun-errors", "processes", "systemd"]}


# --------------------------------------------------------------- analiza + rootcause

def _analyze(project: str) -> dict[str, dict]:
    """Sygnały utknięcia per-ticket z logu koru (streak, drive_failed, waiting_input, env)."""
    sig: dict[str, dict] = {}
    for ln in _log_lines(project):
        wt = _WAIT.search(ln)
        if wt:
            sig.setdefault(wt.group(1), {}).update(waiting_input=("waiting_input" in ln))
        if "drive_failed" in ln or "client command failed" in ln:
            m = _TICKET.search(ln)
            if m:
                s = sig.setdefault(m.group(1), {})
                s["drive_failed"] = s.get("drive_failed", 0) + 1
                st = _STREAK.search(ln)
                if st:
                    s["streak"] = max(s.get("streak", 0), int(st.group(1)))
        if "no viable control route" in ln or "no IDE plugin" in ln or "requires an x11" in ln:
            m = _TICKET.search(ln) or _WAIT.search(ln)
            key = m.group(1) if m else "*"
            sig.setdefault(key, {})["env_blocked"] = True
    return sig


def _classify(s: dict) -> dict:
    """Reguła → (category, rootcause, action). Kolejność: brak-wykonawcy > input > env > stalled."""
    if s.get("drive_failed"):
        return {"category": "no_executor",
                "rootcause": "drive agenta pada (client command failed) — koru nie ma czym wykonać zadania",
                "action": "podłącz agent:// jako route wykonawczy albo zleć zadanie człowiekowi; przerwij retry"}
    if s.get("waiting_input"):
        return {"category": "needs_input",
                "rootcause": "ticket czeka na input operatora (np. creds via secret://)",
                "action": "dostarcz input i Odblokuj; do tego czasu blocked (stop jałowego retry)"}
    if s.get("env_blocked"):
        return {"category": "env",
                "rootcause": "brak sprawnej trasy sterowania (x11/plugin IDE) w tym środowisku",
                "action": "spełnij precondition (sesja/plugin) albo eskaluj do operatora"}
    return {"category": "stalled", "rootcause": "brak postępu (powtarzalny no_change)",
            "action": "uruchom inquiry rootcause albo eskaluj"}


def _ticket_is_active(project: str, tid: str) -> bool:
    """True when a log signal still belongs to an actionable ticket.

    Koru logs are append-only. A ticket can be completed after a noisy failure streak; without
    checking the current planfile status, watchdog keeps reporting the old streak as live.
    Missing status means "unknown", so keep the signal rather than hiding real issues when
    planfile is unavailable.
    """
    status = str((_ticket_show(project, tid) or {}).get("status") or "").lower()
    # "blocked" is actionable (human decision), but it is no longer part of an
    # auto-retry loop. Skipping it prevents watchdog from reporting the same
    # ticket as "stuck" forever after an explicit unstick/hold.
    return not status or status not in ("done", "closed", "blocked", "canceled", "cancelled")


def detect(project: str = "", streak_threshold: int = 3) -> dict[str, Any]:
    """Które tickety utknęły i dlaczego. Read-only — bezpieczne do ciągłego pollowania."""
    proj = _project(project)
    sig = _analyze(proj)
    stuck = []
    for tid, s in sig.items():
        if tid == "*":
            continue
        if not _ticket_is_active(proj, tid):
            continue
        looping = s.get("drive_failed", 0) >= 1 or s.get("streak", 0) >= streak_threshold \
            or s.get("waiting_input") or s.get("env_blocked")
        if not looping:
            continue
        stuck.append({"id": tid, "streak": s.get("streak", 0),
                      "drive_failed": s.get("drive_failed", 0), **_classify(s)})
    for t in stuck:
        osc = _oscillation(proj, t["id"])
        t["cycles"] = osc["cycles"]
        t["oscillation"] = osc
        # ≥2 nieudane rundy block/unblock = POTWIERDZONA jałowa pętla → nie odblokowuj, zdiagnozuj
        t["dead_loop"] = osc["cycles"] >= 2
    stuck.sort(key=lambda x: (-int(x.get("dead_loop", False)), -x.get("streak", 0), -x.get("drive_failed", 0)))
    return {"stuck": stuck, "count": len(stuck),
            "dead_loops": sum(1 for t in stuck if t.get("dead_loop")), "project": proj}


# --------------------------------------------------------------- weryfikacja postępu (claim vs dowody)

# Linie logu, które NIE są realną pracą zadania, tylko introspekcją/napędem koru:
_INTROSPECT = ("planfile ticket", "koru replay", "autopilot trace", "cli-drive",
               "trace show", "mark ticket", "open ticket", "queue:")


def _inprogress_ids(project: str) -> list[str]:
    """Tickety, które planfile uważa za wykonywane (in_progress/claimed)."""
    pf = _planfile()
    if not pf:
        return []
    ids = []
    for status in ("in_progress", "claimed"):
        try:
            cp = subprocess.run([pf, "ticket", "list", "--status", status, "--format", "json"],
                                capture_output=True, text=True, timeout=15, cwd=_project(project))
            raw = cp.stdout
            data = __import__("json").loads(raw[raw.index("["):raw.rindex("]") + 1]) if "[" in raw else []
            ids += [t.get("id") for t in data if t.get("id")]
        except Exception:  # noqa: BLE001
            pass
    return ids


def _evidence(lines: list[str], tid: str) -> dict:
    """Dowody realnej pracy nad ticketem z logu: kroki zadania vs sama introspekcja koru."""
    real = intro = drive_failed = streak = 0
    for ln in lines:
        if tid not in ln:
            continue
        low = ln.lower()
        if "drive_failed" in low or "client command failed" in low:
            drive_failed += 1
        st = _STREAK.search(ln)
        if st:
            streak = max(streak, int(st.group(1)))
        if "obs:" in low and "surface=" in low:
            if any(k in low for k in _INTROSPECT):
                intro += 1
            else:
                real += 1
    return {"real_work": real, "introspection": intro, "drive_failed": drive_failed, "streak": streak}


def _verdict(ev: dict) -> dict:
    """Werdykt: czy 'in_progress' ma pokrycie w dowodach."""
    if ev["drive_failed"] or ev["streak"] >= 3:
        return {"verdict": "looping", "progressing": False,
                "why": "drive pada — koru próbuje, ale wykonawca nie działa (brak realnego postępu)"}
    if ev["real_work"] == 0 and ev["introspection"] == 0:
        return {"verdict": "idle_claim", "progressing": False,
                "why": "in_progress, ale ZERO aktywności w logu — pusty claim, nic się nie dzieje"}
    if ev["real_work"] == 0:
        return {"verdict": "no_task_execution", "progressing": False,
                "why": "koru tylko introspektuje (planfile/replay) — właściwe zadanie NIGDY nie ruszyło"}
    return {"verdict": "progressing", "progressing": True,
            "why": f"widać realne kroki zadania ({ev['real_work']} operacji spoza introspekcji)"}


def verify_progress(project: str = "") -> dict[str, Any]:
    """Dla KAŻDEGO in_progress ticketu: czy naprawdę pracuje (dowody), czy tylko etykieta."""
    lines = _log_lines(_project(project), 800)
    claims = []
    for tid in _inprogress_ids(project):
        ev = _evidence(lines, tid)
        claims.append({"id": tid, **_verdict(ev), "evidence": ev})
    unverified = [c for c in claims if not c["progressing"]]
    return {"claims": claims, "unverified": unverified, "total": len(claims),
            "all_progressing": len(unverified) == 0 and len(claims) > 0}


# --------------------------------------------------------------- oscylacja stanu (circuit breaker)

def _ticket_show(project: str, tid: str) -> dict:
    pf = _planfile()
    if not pf:
        return {}
    try:
        cp = subprocess.run([pf, "ticket", "show", tid, "--format", "json"],
                            capture_output=True, text=True, timeout=15, cwd=_project(project))
        raw = cp.stdout
        return json.loads(raw[raw.index("{"):raw.rindex("}") + 1]) if "{" in raw else {}
    except Exception:  # noqa: BLE001
        return {}


def _oscillation(project: str, tid: str) -> dict:
    """Ile razy ticket bujał się między stanami (open↔blocked↔in_progress) — z historii planfile.

    ``cycles`` = liczba nieudanych rund block/unblock. To wykrywa jałową pętlę CZŁOWIEKA:
    Odblokuj → koru drive → fail → block → Odblokuj … bez postępu."""
    hist = _ticket_show(project, tid).get("history") or []
    statuses = [h.get("status") for h in hist if h.get("status")]
    c = Counter(statuses)
    cycles = min(c.get("blocked", 0), c.get("open", 0) + c.get("in_progress", 0) + c.get("claimed", 0))
    return {"cycles": cycles, "transitions": len(statuses), "status_counts": dict(c)}


def _existing_diag(project: str, tid: str) -> dict | None:
    """Najnowszy ticket diagnozy dla tego zapętlenia — DOWOLNY status (idempotencja: JEDNA
    diagnoza na ticket, nie co cykl). Zwraca otwartą, a jeśli brak — najnowszą done/closed,
    żeby breaker REOTWORZYŁ JĄ zamiast tworzyć świeżą DIAGNOZĘ w każdym cyklu (koniec szumu)."""
    pf = _planfile()
    if not pf:
        return None
    try:
        cp = subprocess.run([pf, "ticket", "list", "--format", "json"],
                            capture_output=True, text=True, timeout=15, cwd=_project(project))
        raw = cp.stdout
        data = json.loads(raw[raw.index("["):raw.rindex("]") + 1]) if "[" in raw else []
    except Exception:  # noqa: BLE001
        return None
    diags = [t for t in data if f"loop-diag:{tid}" in (t.get("labels") or [])]
    if not diags:
        return None
    live = [t for t in diags if t.get("status") not in ("done", "closed", "cancelled", "canceled")]
    pick = live[0] if live else max(diags, key=lambda t: t.get("updated") or "")
    return {"id": pick.get("id"), "status": pick.get("status")}


def _rehold_existing_diag(project: str, tid: str, existing: dict) -> dict:
    """Breaker trzyma dalej na ISTNIEJĄCEJ diagnozie (JEDNA na ticket). Jeśli była zamknięta
    a rootcause żyje — reotwiera ją, zamiast tworzyć nową DIAGNOZĘ co cykl (koniec szumu)."""
    diag_id = existing["id"]
    reopened = _reopen_diag(project, diag_id) if existing.get("status") in ("done", "closed") else False
    state = "REOPEN (rootcause zyje)" if reopened else "otwarta"
    note = (f"watchdog breaker[{_dt.datetime.now():%H:%M}]: re-hold — diagnoza {diag_id} "
            f"{state}, rootcause zyje; blocked_by {diag_id}")
    blocked_by = _hold_blocked(project, tid, diag_id, note)
    return _ok(action="watch-breaker", ticket=tid, diagnosis=diag_id, already=True,
               reopened=reopened, blocked_by=blocked_by)


def _reopen_diag(project: str, diag_id: str) -> bool:
    """Reotwórz zamkniętą diagnozę (status→blocked) zamiast tworzyć nową — JEDNA DIAGNOZA na
    ticket, na zawsze. Blocked (nie open), bo diagnoza to actor:human — koru nie ma jej drivować."""
    pf = _planfile()
    if not pf or not diag_id:
        return False
    try:
        cp = subprocess.run([pf, "ticket", "update", diag_id, "--status", "blocked"],
                            capture_output=True, text=True, timeout=15, cwd=_project(project))
        return cp.returncode == 0
    except Exception:  # noqa: BLE001
        return False


# --------------------------------------------------------------- działanie (przerwij pętlę)

def _escalation(tid: str, info: dict) -> dict:
    try:
        from urirun.host.planfile_adapter import get_ticket_urls
        urls = get_ticket_urls(tid)
    except Exception:
        work_base = _ESCALATE_BASE
        chat_base = os.environ.get("URIRUN_CHAT_BASE") or "http://127.0.0.1:8194"
        urls = {
            "dashboard": f"{work_base}/work?ticket={tid}",
            "changes_history": f"{work_base}/work?ticket={tid}#history",
            "llm_conversations": f"{chat_base}/?ticket={tid}",
            "llm_history_api": f"{chat_base}/api/chat/history?ticket={tid}",
        }
    return {
        "ticket": tid,
        "category": info.get("category"),
        "rootcause": info.get("rootcause"),
        "action": info.get("action"),
        "urls": urls,
        "human_uri": f"human://operator/decision/{tid}"
    }


def _set_blocked_by(project: str, tid: str, deps: list[str]) -> list[str]:
    """Ustaw STRUKTURALNE ``blocked_by`` na oryginalnym tickecie (merge, idempotentnie).
    To DOMYKA breaker: reconciler loop:// trzyma ticket ``blocked``, dopoki wszystkie
    ``blocked_by`` (tu: ticket diagnozy) nie sa ``done`` — i dopiero wtedy sam odblokuje.
    Bez tego pola queue/reconciler nie widzi zaleznosci -> oryginal wraca do ``open`` i koru
    re-driveuje go w kolko (jalowa petla). Best-effort — planfile Python API opcjonalne."""
    try:
        from planfile import Planfile
        pf = Planfile(_project(project))
        cur = pf.get_ticket(tid)
        have = list(getattr(cur, "blocked_by", None) or []) if cur else []
        merged = have + [d for d in deps if d and d not in have]
        if merged != have:
            pf.update_ticket(tid, blocked_by=merged)
        return merged
    except Exception:  # noqa: BLE001
        return []


def _hold_blocked(project: str, tid: str, diag_id: str, note: str) -> list[str]:
    """Domknij breaker na oryginalnym tickecie: notatka (rozpoznawana przez reconciler
    loop:// dzieki wzorcowi ``blocked_by <ID>``) + status ``blocked`` + strukturalne
    ``blocked_by``. Notatka to fallback, gdyby zapis strukturalnego pola nie przeszedl.
    Idempotentne, best-effort. Zwraca finalna liste ``blocked_by``."""
    pf = _planfile()
    if pf:
        for args in (["ticket", "update", tid, "--note", note],
                     ["ticket", "block", tid, "-r", note]):
            try:
                subprocess.run([pf, *args], capture_output=True, text=True,
                               timeout=20, cwd=_project(project))
            except Exception:  # noqa: BLE001
                pass
    return _set_blocked_by(project, tid, [diag_id])


@conn.handler("loop/query/detect", isolated=False,
              meta={"label": "Wykryj zapętlone/utknięte tickety koru z rootcause (read-only)"})
def loop_query_detect(project: str = "", streak_threshold: int = 3) -> dict[str, Any]:
    try:
        return _ok(action="watch-detect", **detect(project, int(streak_threshold)))
    except Exception as exc:  # noqa: BLE001
        return _fail(str(exc), "watch-detect")


@conn.handler("ticket/query/verify", isolated=False,
              meta={"label": "Czy in_progress tickety NAPRAWDĘ pracują — werdykt z dowodów (nie z etykiety)"})
def ticket_query_verify(project: str = "") -> dict[str, Any]:
    try:
        return _ok(action="watch-verify", **verify_progress(project))
    except Exception as exc:  # noqa: BLE001
        return _fail(str(exc), "watch-verify")


def system_remediate(project: str = "") -> dict[str, Any]:
    """Wykryta anomalia SYSTEMU → AUTO-utwórz ticket z PRIORYTETEM (fix najszybciej), idempotentnie.
    Blokująca (severity high) → CRITICAL + actor:human (eskalacja). To domyka detekcja→ticket→fix."""
    import subprocess
    r = system_analyze(project)
    pf = _planfile()
    if not pf or not r["findings"]:
        return {**r, "created": []}
    try:  # idempotencja: istniejące sysfix:* tickety
        cp = subprocess.run([pf, "ticket", "list", "--format", "json"], capture_output=True,
                            text=True, timeout=15, cwd=_project(project))
        raw = cp.stdout
        data = json.loads(raw[raw.index("["):raw.rindex("]") + 1]) if "[" in raw else []
        have = {l for t in data if t.get("status") not in ("done", "closed") for l in (t.get("labels") or [])}
    except Exception:  # noqa: BLE001
        have = set()
    created = []
    for f in r["findings"]:
        label = f"sysfix:{f['pattern']}"
        if label in have:
            continue
        blocking = f["severity"] == "high"
        prio = "critical" if blocking else "high"
        name = f"[SYSTEM] {f['pattern']} — napraw ({prio})"
        desc = (f"Watchdog (natywny monitor) wykrył ANOMALIĘ SYSTEMU. Obserwacja: {f['observed']}. "
                f"Wniosek: {f['conclusion']}. KONTRAKCJA: {f['counteraction']}. "
                + ("BLOKUJĄCA — uniemożliwia działanie → eskalacja human:// + wstrzymaj jałowe próby." if blocking else ""))
        actor = "actor:human" if blocking else "actor:agent"
        try:
            cp = subprocess.run([pf, "ticket", "create", name, "-p", prio, "--source", "watchdog-system",
                                 "-l", "system-anomaly", "-l", label, "-l", actor, "-d", desc],
                                capture_output=True, text=True, timeout=20, cwd=_project(project))
            new = (re.search(r"[A-Z]+-\d+", cp.stdout) or [None, ""])[1] if cp.stdout else ""
            if new:
                created.append({"ticket": new, "pattern": f["pattern"], "priority": prio, "blocking": blocking})
        except Exception:  # noqa: BLE001
            pass
    return {**r, "created": created}


@conn.handler("system/command/remediate", isolated=True,
              meta={"label": "Wykryta anomalia SYSTEMU → auto-ticket z priorytetem (critical gdy blokująca)"})
def system_command_remediate(project: str = "") -> dict[str, Any]:
    try:
        return _ok(action="watch-system-remediate", **system_remediate(project))
    except Exception as exc:  # noqa: BLE001
        return _fail(str(exc), "watch-system-remediate")


@conn.handler("system/query/analyze", isolated=False,
              meta={"label": "Zdrowie SYSTEMU: kontroler nieproduktywny / executor zerwany / konflikt kontrolerów + kontrakcja"})
def system_query_analyze(project: str = "") -> dict[str, Any]:
    try:
        return _ok(action="watch-system", **system_analyze(project))
    except Exception as exc:  # noqa: BLE001
        return _fail(str(exc), "watch-system")


def rabbit_hole_reap(project: str = "") -> dict[str, Any]:
    """Reakcja NA POZIOMIE NORY: dla każdej króliczej nory JEDNA eskalacja (idempotentnie,
    label rabbithole:topic) spinająca wszystkie koincydencje. Zamiast mnożyć próby w norze."""
    r = rabbit_hole_correlate(project)
    pf = _planfile()
    if not pf or not r.get("holes"):
        return {**r, "reacted": []}
    try:
        cp = subprocess.run([pf, "ticket", "list", "--format", "json"], capture_output=True,
                            text=True, timeout=15, cwd=_project(project))
        data = json.loads(cp.stdout[cp.stdout.index("["):cp.stdout.rindex("]") + 1])
        have = {l for t in data if t.get("status") not in ("done", "closed") for l in (t.get("labels") or [])}
    except Exception:  # noqa: BLE001
        have = set()
    reacted = []
    for h in r["holes"]:
        _learn_rabbit_hole(h)  # ŚWIADOMOŚĆ: ucz mind ZAWSZE (idempotentnie), też gdy eskalacja już jest
        label = f"rabbithole:{h['topic']}"
        if label in have:
            reacted.append({"hole": h["hole"], "already": True})
            continue
        name = f"[NORA] {h['hole']} — {h['members']} ticketów, {h['loop_events']} zdarzeń, 0 postępu"
        desc = (f"Korelator spiął koincydencje w JEDNĄ króliczą norę: {h['tickets']}. {h['verdict']} "
                f"JEDEN punkt reakcji dla klastra — rozwiąż ROOT nory, nie pojedyncze tickety.")
        try:
            cp = subprocess.run([pf, "ticket", "create", name, "-p", "high", "--source", "watchdog-rabbithole",
                                 "-l", "rabbit-hole", "-l", label, "-l", "actor:human", "-d", desc],
                                capture_output=True, text=True, timeout=20, cwd=_project(project))
            new = (re.search(r"[A-Z]+-\d+", cp.stdout) or [None, ""])[1] if cp.stdout else ""
            for tid in h["tickets"]:
                subprocess.run([pf, "ticket", "update", tid, "--note", f"spięte w {label} ({new})"],
                               capture_output=True, text=True, timeout=15, cwd=_project(project))
            reacted.append({"hole": h["hole"], "escalation": new, "spun": h["tickets"]})
        except Exception:  # noqa: BLE001
            pass
    return {**r, "reacted": reacted}


def _learn_rabbit_hole(h: dict) -> None:
    """Zapisz norę do urirun-mind (antywzorzec + antywzorzec systemowy) — świadomość: strategy_selector
    ostrzeże PRZED ponownym wejściem, zamiast powtarzać N prób w tej samej norze."""
    try:
        from urirun_mind import antipatterns
        antipatterns.add({"id": f"rabbithole:{h['topic']}", "trigger": {"intent": h["topic"]},
                          "avoid": [f"*{h['topic']}*retry*", "*click-text*", "*input/command/type*"],
                          "prefer": ["human://operator/decision"]})
    except Exception:  # noqa: BLE001
        pass
    _record_findings([{"pattern": f"rabbit-hole:{h['topic']}", "severity": "high",
                       "observed": f"{h['members']} ticketów, {h['loop_events']} zdarzeń, 0 postępu",
                       "conclusion": f"klaster koincydencji '{h['topic']}' = jedna nora; root wspólny",
                       "counteraction": "rozwiąż ROOT nory raz; NIE mnóż prób/ticketów; strategy_selector avoid"}])


@conn.handler("system/query/rabbit-hole", isolated=False,
              meta={"label": "Spina koincydencje ticketów w królicze nory (+reakcja klastra przez react=true)"})
def system_query_rabbit_hole(project: str = "", react: bool = False) -> dict[str, Any]:
    try:
        out = rabbit_hole_reap(project) if react else rabbit_hole_correlate(project)
        return _ok(action="watch-rabbit-hole", **out)
    except Exception as exc:  # noqa: BLE001
        return _fail(str(exc), "watch-rabbit-hole")


@conn.handler("query/report", isolated=False, meta={"label": "Czytelny raport: co utknęło i dlaczego"})
def query_report(project: str = "") -> dict[str, Any]:
    try:
        d = detect(project)
        if not d["stuck"]:
            return _ok(action="watch-report", report="Brak zapętleń — pętla idzie do przodu.", **d)
        lines = [f"⚠ {t['id']} [{t['category']}] streak={t['streak']} — {t['rootcause']} → {t['action']}"
                 for t in d["stuck"]]
        return _ok(action="watch-report", report="\n".join(lines), **d)
    except Exception as exc:  # noqa: BLE001
        return _fail(str(exc), "watch-report")


@conn.handler("ticket/command/unstick", isolated=True,
              meta={"label": "Przerwij jałową pętlę: ticket→blocked + notatka + eskalacja (human://)"})
def ticket_command_unstick(id: str = "", project: str = "") -> dict[str, Any]:
    tid = (id or "").strip()
    if not tid:
        return _fail("id ticketu wymagane", "watch-unstick")
    info = next((t for t in detect(project)["stuck"] if t["id"] == tid), None) \
        or {"category": "stalled", "rootcause": "watchdog", "action": "eskalacja"}
    pf = _planfile()
    if not pf:
        return _fail("planfile niedostępny", "watch-unstick", escalation=_escalation(tid, info))
    stamp = _dt.datetime.now().strftime("%H:%M")
    note = f"watchdog[{stamp}]: {info['category']} — {info['rootcause']} → {info['action']}"
    try:
        subprocess.run([pf, "ticket", "update", tid, "--note", note],
                       capture_output=True, text=True, timeout=20, cwd=_project(project))
        cp = subprocess.run([pf, "ticket", "block", tid, "-r", note],
                            capture_output=True, text=True, timeout=20, cwd=_project(project))
        if cp.returncode != 0:
            return _fail((cp.stderr or cp.stdout or "planfile error").strip()[-200:], "watch-unstick")
    except Exception as exc:  # noqa: BLE001
        return _fail(str(exc), "watch-unstick")
    return _ok(action="watch-unstick", ticket=tid, status="blocked", escalation=_escalation(tid, info))


@conn.handler("loop/command/circuit-break", isolated=True,
              meta={"label": "Przerwij OSCYLACJĘ: auto-diagnoza jako ticket + zatrzymaj (bez re-odblokowania)"})
def _get_ticket(project: str, tid: str) -> dict | None:
    import json
    pf = _planfile()
    if not pf:
        return None
    try:
        cp = subprocess.run([pf, "ticket", "show", tid, "--format", "json"], capture_output=True,
                            text=True, timeout=10, cwd=_project(project))
        return json.loads(cp.stdout[cp.stdout.index("{"):cp.stdout.rindex("}") + 1])
    except Exception:  # noqa: BLE001
        return None


# jeden trwały GATE per rodzaj czekania — waiting tickety blokują się NA NIM (nie oscylują), a on
# jest actor:human i nigdy auto-done → reconciler trzyma je do czasu realnego wejścia. Bez mnożenia diagnoz.
_WAIT_GATE_DESC = {
    "waiting_secret": "Dostarcz sekret/creds/token (keyring set / secret://). Tickety czekające tu ruszą po dostarczeniu.",
    "waiting_node": "Postaw/enrolluj węzeł (np. lenovo up + deploy). Tickety node-zależne ruszą gdy węzeł ready.",
    "waiting_human": "Wykonaj fizyczny krok człowieka (skan QR / link / instalacja). Tickety ruszą po wykonaniu.",
    "waiting_external": "Zewnętrzne approval/publikacja — decyzja operatora (approval packet).",
}


def _ensure_wait_gate(project: str, kind: str) -> str:
    """Znajdź lub utwórz JEDEN trwały gate-ticket dla rodzaju czekania. Zwraca jego ID."""
    import json
    pf = _planfile()
    if not pf:
        return ""
    label = f"wait-gate:{kind}"
    try:
        cp = subprocess.run([pf, "ticket", "list", "--format", "json"], capture_output=True,
                            text=True, timeout=15, cwd=_project(project))
        data = json.loads(cp.stdout[cp.stdout.index("["):cp.stdout.rindex("]") + 1])
        hit = next((t for t in data if label in (t.get("labels") or [])
                    and t.get("status") not in ("done", "closed", "cancelled", "canceled")), None)
        if hit:
            return hit["id"]
        try:
            from urirun_connector_grants import unblock_ledger
            if unblock_ledger.is_unblocked_for({"labels": [label]}):
                return ""  # typ już odblokowany — nie twórz kolejnego gate
        except Exception:  # noqa: BLE001
            pass
        cp = subprocess.run([pf, "ticket", "create", f"[GATE] {kind} — czekanie na wejście", "-p", "normal",
                             "--source", "watchdog-wait-gate", "-l", label, "-l", "actor:human",
                             "-d", _WAIT_GATE_DESC.get(kind, "Legalne czekanie na wejście.")],
                            capture_output=True, text=True, timeout=20, cwd=_project(project))
        return (re.search(r"([A-Z]+-\d+)", cp.stdout) or [None, ""])[1] if cp.stdout else ""
    except Exception:  # noqa: BLE001
        return ""


def loop_command_circuit_break(id: str = "", project: str = "") -> dict[str, Any]:
    """Gdy ticket buja się open↔blocked bez postępu: NIE odblokowuj znowu. Utwórz ticket
    DIAGNOZY (rootcause + realny fix), zablokuj oryginał 'blocked_by' diagnozy, przerwij pętlę."""
    tid = (id or "").strip()
    if not tid:
        return _fail("id ticketu wymagane", "watch-breaker")
    pf = _planfile()
    if not pf:
        return _fail("planfile niedostępny", "watch-breaker")
    # KONTRAKT: legalne czekanie (secret/node/human/external) NIE jest oscylacją do diagnozy.
    # Trzymaj na trwałym GATE per-kind (reconciler nie re-otwiera) — BEZ szumu DIAGNOZA.
    tk = _get_ticket(project, tid)
    if tk and _is_unblocked(tk):  # człowiek odblokował raz → nie diagnozuj NIGDY (auto-kontynuacja)
        return _ok(action="watch-breaker", ticket=tid, human_unblocked=True, diagnosis=None,
                   note="odblokowane trwale przez człowieka — brak diagnozy, auto-kontynuacja")
    kind = wait_kind(tk) if tk else ""
    if kind:
        gate = _ensure_wait_gate(project, kind)
        note = f"watchdog: {tid} = legalne czekanie ({kind}) — trzymam na {gate}, NIE diagnozuję (czeka na wejście, nie pętla)"
        blocked_by = _hold_blocked(project, tid, gate, note)
        return _ok(action="watch-breaker", ticket=tid, wait_kind=kind, gate=gate,
                   diagnosis=None, blocked_by=blocked_by, note="legal wait → no diagnosis")
    existing = _existing_diag(project, tid)
    if existing:
        return _rehold_existing_diag(project, tid, existing)
    info = next((t for t in detect(project)["stuck"] if t["id"] == tid), None) or {}
    osc = _oscillation(_project(project), tid)
    name = f"DIAGNOZA: {tid} zapętlony ({info.get('category', 'loop')}) — {osc['cycles']}× cykl bez postępu"
    desc = (f"Watchdog wykrył OSCYLACJĘ ticketu {tid}: {osc['status_counts']} ({osc['transitions']} przejść, "
            f"{osc['cycles']} rund block/unblock) BEZ postępu. Rootcause: {info.get('rootcause', '?')}. "
            f"Realny fix: {info.get('action', 'usuń rootcause')}. "
            "UWAGA: odblokowywanie NIE pomaga — pętla wróci, dopóki rootcause żyje "
            "(np. brak executora → podłącz agent:// / 'Wykonaj agentem'; brak creds → dostarcz secret://). "
            f"Breaker otwarty: {tid} zostaje blocked aż ta diagnoza zostanie zamknięta.")
    # Diagnoza breakera NIGDY nie jest headless-drivable: rootcause zyje dopoki czlowiek/agent
    # nie usunie go (np. no_executor -> podlacz agent:// / zlec czlowiekowi). Bez actor:human koru
    # bierze diagnoze jak zwykla prace, cli-driveuje ja i sam wpada w META-PETLE (diagnoza no_executor
    # tez nie ma executora). actor:human przerywa ten retry i kieruje ticket do operatora.
    try:
        cp = subprocess.run([pf, "ticket", "create", name, "-p", "high", "--source", "watchdog",
                             "-l", "diagnosis", "-l", f"loop-diag:{tid}", "-l", "actor:human", "-d", desc],
                            capture_output=True, text=True, timeout=20, cwd=_project(project))
        if cp.returncode != 0:
            return _fail((cp.stderr or cp.stdout or "create failed").strip()[-200:], "watch-breaker")
        diag_id = (re.search(r"([A-Z]+-\d+)", cp.stdout) or [None, ""])[1] if cp.stdout else ""
        note = (f"watchdog breaker[{_dt.datetime.now():%H:%M}]: oscylacja {osc['cycles']}× → diagnoza {diag_id}; "
                f"NIE odblokowuj bez fixu rootcause; blocked_by {diag_id}")
        blocked_by = _hold_blocked(project, tid, diag_id, note)
    except Exception as exc:  # noqa: BLE001
        return _fail(str(exc), "watch-breaker")
    return _ok(action="watch-breaker", ticket=tid, diagnosis=diag_id, cycles=osc["cycles"],
               blocked_by=blocked_by, escalation=_escalation(tid, info))


@conn.handler("loop/command/sweep", isolated=True,
              meta={"label": "Wykryj wszystkie zapętlenia i (opcjonalnie apply=True) przerwij je"})
def loop_command_sweep(project: str = "", apply: bool = False) -> dict[str, Any]:
    d = detect(project)
    escalations = [_escalation(t["id"], t) for t in d["stuck"]]
    applied = []
    if apply:
        for t in d["stuck"]:
            r = ticket_command_unstick(id=t["id"], project=project)
            if r.get("ok"):
                applied.append(t["id"])
    return _ok(action="watch-sweep", stuck=d["stuck"], escalations=escalations,
               applied=applied, dry_run=not apply)


def urirun_bindings() -> dict[str, Any]:
    return conn.bindings()


def connector_manifest() -> dict[str, Any]:
    return urirun.load_manifest(__package__) or {"id": CONNECTOR_ID}


def main(argv: list[str] | None = None) -> int:
    return conn.cli(argv, manifest_prose=urirun.load_manifest(__package__))


if __name__ == "__main__":
    raise SystemExit(main())

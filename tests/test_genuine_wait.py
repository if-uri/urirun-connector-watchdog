"""KONTRAKT: watchdog rozróżnia legalne czekanie na człowieka od rabbit-hole. Pinuje przypadki bagna."""
from urirun_connector_watchdog import core

def _t(name, labels=None, desc=""):
    return {"id": "X", "name": name, "labels": labels or [], "description": desc}

# ── legalne czekanie → NIE rabbit-hole (wait_kind != "") ──
def test_signal_qr_is_human_wait():
    assert core.wait_kind(_t("Zalinkuj konto Signal (skan QR telefonem)", ["actor:human"])) == "waiting_human"

def test_signal_cli_install_is_wait():
    assert core.wait_kind(_t("Zainstaluj signal-cli na lenovo", ["actor:human"])) != ""

def test_imap_creds_is_secret_wait():
    assert core.wait_kind(_t("email spam review via IMAP", ["email-test"])) == "waiting_secret"

def test_keyring_is_secret_wait():
    assert core.wait_kind(_t("email review", desc="wymaga keyring set mail app-password")) == "waiting_secret"

def test_thunderbird_lenovo_is_wait():
    assert core.wait_kind(_t("Wznów email/Thunderbird na lenovo")) != ""

def test_lenovo_deploy_is_node_wait():
    assert core.wait_kind(_t("Wdróż signal:// na węzeł lenovo (nodeadmin)", ["node:lenovo", "deploy"])) == "waiting_node"

def test_pypi_token_is_secret_wait():
    assert core.wait_kind(_t("Opublikuj signal:// na PyPI", ["needs-human:pypi-token"])) == "waiting_secret"

# ── zapętlenie → rabbit-hole candidate (wait_kind == "") ──
def test_no_markers_is_rabbit_hole_candidate():
    assert core.wait_kind(_t("refaktor helpera XYZ", ["code"])) == ""

def test_repeated_uri_failure_is_candidate():
    assert core.wait_kind(_t("kvm click nie działa 5× cykl", ["kvm", "code"])) == ""

def test_ok_true_effect_false_is_candidate():
    assert core.wait_kind(_t("runtime-lies-ok: abs/command/click zwraca ok bez efektu", ["refactor"])) == ""

# ── integ: klaster samych human-waits → BRAK nory; klaster kodu z pętlą → NORA ──
def test_correlate_excludes_human_wait_cluster(monkeypatch):
    tix = [_t2("A", "Zalinkuj Signal QR telefonem", "signal", ["actor:human"]),
           _t2("B", "Zainstaluj signal-cli na lenovo", "signal", ["actor:human"])]
    monkeypatch.setattr(core, "_ticket_list", lambda p: tix) if hasattr(core, "_ticket_list") else None
    # bezpośrednio: oba to human-wait → wykluczone
    assert all(core._genuine_human_wait(t) for t in tix)

def _t2(tid, name, topic, labels):
    return {"id": tid, "name": name, "labels": labels, "description": "", "status": "blocked"}


def test_circuit_break_skips_legal_wait(monkeypatch, tmp_path):
    """KONTRAKT: circuit_break dla legal-wait NIE tworzy DIAGNOZA — trzyma na gate."""
    calls = []
    monkeypatch.setattr(core, "_planfile", lambda: "pf")
    monkeypatch.setattr(core, "_get_ticket", lambda p, t: {"id": t, "name": "Opublikuj na PyPI", "labels": ["needs-human:pypi-token"]})
    monkeypatch.setattr(core, "_ensure_wait_gate", lambda p, k: "GATE-1")
    monkeypatch.setattr(core, "_hold_blocked", lambda p, t, g, n: [g])
    monkeypatch.setattr(core, "_existing_diag", lambda p, t: (_ for _ in ()).throw(AssertionError("nie powinno dojść do diagnozy")))
    r = core.loop_command_circuit_break("IFURI-043", "")
    assert r["diagnosis"] is None and r["wait_kind"] == "waiting_secret" and r["gate"] == "GATE-1"

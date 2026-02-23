"""Tests for auto_scaler.py"""
import time
import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from auto_scaler import (
    add_policy, get_policy, remove_policy, list_policies,
    ingest_metric, get_recent_metrics,
    evaluate_scaling, apply_scaling, evaluate_and_apply,
    get_scaling_history, get_replica_state, get_scaling_summary,
    ScaleAction, ScaleReason, _get_db,
)


@pytest.fixture
def db(tmp_path):
    return _get_db(tmp_path / "test_scaler.db")


# ---------------------------------------------------------------------------
# Policy management
# ---------------------------------------------------------------------------

def test_add_policy(db):
    p = add_policy("api", 2, 10, 70.0, 100.0, db=db)
    assert p.service_name == "api"
    assert p.min_replicas == 2
    assert p.max_replicas == 10


def test_add_policy_upsert(db):
    add_policy("svc", 1, 5, 60.0, 50.0, db=db)
    add_policy("svc", 2, 8, 75.0, 80.0, db=db)
    p = get_policy("svc", db=db)
    assert p.max_replicas == 8


def test_add_policy_invalid_min(db):
    with pytest.raises(ValueError):
        add_policy("x", 0, 5, 70.0, 50.0, db=db)


def test_add_policy_invalid_max(db):
    with pytest.raises(ValueError):
        add_policy("x", 5, 3, 70.0, 50.0, db=db)


def test_add_policy_invalid_cpu(db):
    with pytest.raises(ValueError):
        add_policy("x", 1, 5, 0.0, 50.0, db=db)


def test_add_policy_invalid_rps(db):
    with pytest.raises(ValueError):
        add_policy("x", 1, 5, 70.0, -1.0, db=db)


def test_get_policy_not_found(db):
    assert get_policy("nonexistent", db=db) is None


def test_remove_policy(db):
    add_policy("to-del", 1, 5, 70.0, 50.0, db=db)
    ok = remove_policy("to-del", db=db)
    assert ok
    assert get_policy("to-del", db=db) is None


def test_list_policies(db):
    add_policy("p1", 1, 5, 70.0, 50.0, db=db)
    add_policy("p2", 2, 8, 80.0, 100.0, db=db)
    policies = list_policies(db=db)
    names = [p.service_name for p in policies]
    assert "p1" in names and "p2" in names


# ---------------------------------------------------------------------------
# Metric ingestion
# ---------------------------------------------------------------------------

def test_ingest_metric(db):
    snap = ingest_metric("api", cpu_pct=55.0, rps=80.0, replicas=3, db=db)
    assert snap.service_name == "api"
    assert snap.cpu_pct == 55.0


def test_get_recent_metrics(db):
    ingest_metric("svc2", 40.0, 60.0, 2, db=db)
    ingest_metric("svc2", 50.0, 70.0, 2, db=db)
    snaps = get_recent_metrics("svc2", window_secs=300, db=db)
    assert len(snaps) >= 2


def test_get_recent_metrics_empty(db):
    snaps = get_recent_metrics("unknown-svc", db=db)
    assert snaps == []


# ---------------------------------------------------------------------------
# Scaling evaluation
# ---------------------------------------------------------------------------

def test_evaluate_scale_up_cpu(db):
    add_policy("high-cpu", 1, 10, 70.0, 500.0, cooldown_secs=0, db=db)
    ingest_metric("high-cpu", cpu_pct=95.0, rps=100.0, replicas=2, db=db)
    d = evaluate_scaling("high-cpu", db=db)
    assert d.action == ScaleAction.SCALE_UP.value
    assert d.new_replicas > d.current_replicas


def test_evaluate_scale_up_rps(db):
    add_policy("high-rps", 1, 10, 90.0, 50.0, cooldown_secs=0, db=db)
    ingest_metric("high-rps", cpu_pct=50.0, rps=200.0, replicas=2, db=db)
    d = evaluate_scaling("high-rps", db=db)
    assert d.action == ScaleAction.SCALE_UP.value


def test_evaluate_scale_down(db):
    add_policy("low-load", 1, 10, 70.0, 100.0, cooldown_secs=0, db=db)
    ingest_metric("low-load", cpu_pct=10.0, rps=5.0, replicas=8, db=db)
    d = evaluate_scaling("low-load", db=db)
    assert d.action == ScaleAction.SCALE_DOWN.value
    assert d.new_replicas < d.current_replicas


def test_evaluate_no_action_within_band(db):
    add_policy("normal", 1, 10, 70.0, 100.0, cooldown_secs=0, db=db)
    ingest_metric("normal", cpu_pct=70.0, rps=100.0, replicas=3, db=db)
    d = evaluate_scaling("normal", db=db)
    assert d.action == ScaleAction.NONE.value


def test_evaluate_at_max_no_scale_up(db):
    add_policy("at-max", 1, 3, 70.0, 100.0, cooldown_secs=0, db=db)
    ingest_metric("at-max", cpu_pct=99.0, rps=999.0, replicas=3, db=db)
    d = evaluate_scaling("at-max", db=db)
    assert d.action == ScaleAction.NONE.value
    assert d.reason == ScaleReason.AT_MAX.value


def test_evaluate_at_min_no_scale_down(db):
    add_policy("at-min", 2, 10, 70.0, 100.0, cooldown_secs=0, db=db)
    ingest_metric("at-min", cpu_pct=1.0, rps=0.1, replicas=2, db=db)
    d = evaluate_scaling("at-min", db=db)
    assert d.action == ScaleAction.NONE.value
    assert d.reason == ScaleReason.AT_MIN.value


def test_evaluate_cooldown(db):
    add_policy("cooling", 1, 10, 70.0, 100.0, cooldown_secs=3600, db=db)
    ingest_metric("cooling", cpu_pct=95.0, rps=500.0, replicas=2, db=db)
    # Simulate a recent scale event
    db.execute(
        "INSERT OR REPLACE INTO replica_state (service_name,replicas,last_scale_ts) VALUES (?,?,?)",
        ("cooling", 2, time.time() - 10),
    )
    db.commit()
    d = evaluate_scaling("cooling", db=db)
    assert d.reason == ScaleReason.COOLDOWN.value


def test_evaluate_no_policy(db):
    d = evaluate_scaling("no-policy-svc", db=db)
    assert d.action == ScaleAction.NONE.value


def test_evaluate_no_metrics(db):
    add_policy("no-metrics-svc", 1, 5, 70.0, 50.0, db=db)
    d = evaluate_scaling("no-metrics-svc", db=db)
    assert d.reason == ScaleReason.NO_METRICS.value


# ---------------------------------------------------------------------------
# Apply scaling
# ---------------------------------------------------------------------------

def test_apply_scaling(db):
    add_policy("apply-test", 1, 10, 70.0, 100.0, cooldown_secs=0, db=db)
    ingest_metric("apply-test", cpu_pct=95.0, rps=500.0, replicas=2, db=db)
    result = apply_scaling("apply-test", 5, db=db)
    assert result["new_replicas"] == 5
    rs = get_replica_state("apply-test", db=db)
    assert rs["replicas"] == 5


def test_apply_creates_history_entry(db):
    add_policy("hist-svc", 1, 10, 70.0, 100.0, cooldown_secs=0, db=db)
    ingest_metric("hist-svc", cpu_pct=95.0, rps=500.0, replicas=2, db=db)
    apply_scaling("hist-svc", 4, db=db)
    history = get_scaling_history("hist-svc", db=db)
    assert len(history) >= 1


# ---------------------------------------------------------------------------
# Scaling history
# ---------------------------------------------------------------------------

def test_scaling_history_order(db):
    add_policy("order-svc", 1, 10, 70.0, 100.0, cooldown_secs=0, db=db)
    for i in range(3, 6):
        ingest_metric("order-svc", cpu_pct=95.0, rps=500.0, replicas=i, db=db)
        apply_scaling("order-svc", i + 1, db=db)
    history = get_scaling_history("order-svc", limit=10, db=db)
    assert len(history) >= 3


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def test_scaling_summary(db):
    add_policy("sum1", 1, 5, 70.0, 50.0, db=db)
    add_policy("sum2", 2, 8, 80.0, 100.0, db=db)
    summary = get_scaling_summary(db=db)
    names = [s["service"] for s in summary]
    assert "sum1" in names
    assert "sum2" in names


def test_step_limits(db):
    add_policy("stepped", 1, 10, 70.0, 100.0, cooldown_secs=0,
               scale_up_step=3, db=db)
    ingest_metric("stepped", cpu_pct=95.0, rps=500.0, replicas=2, db=db)
    d = evaluate_scaling("stepped", db=db)
    assert d.new_replicas == 5  # 2 + 3

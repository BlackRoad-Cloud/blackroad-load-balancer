"""
BlackRoad Auto Scaler
Production-quality horizontal auto-scaler with CPU/RPS-based policies,
cooldown enforcement, metric ingestion, and scaling history. SQLite backed.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional

DB_PATH = Path.home() / ".blackroad" / "auto_scaler.db"

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ScaleAction(str, Enum):
    SCALE_UP   = "scale_up"
    SCALE_DOWN = "scale_down"
    NONE       = "none"


class ScaleReason(str, Enum):
    HIGH_CPU     = "high_cpu"
    LOW_CPU      = "low_cpu"
    HIGH_RPS     = "high_rps"
    LOW_RPS      = "low_rps"
    AT_MIN       = "at_min_replicas"
    AT_MAX       = "at_max_replicas"
    COOLDOWN     = "in_cooldown"
    NO_METRICS   = "no_metrics"
    WITHIN_BAND  = "within_target_band"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ScalingPolicy:
    service_name: str
    min_replicas: int
    max_replicas: int
    target_cpu_pct: float          # 0-100
    target_rps: float              # requests per second
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    cooldown_secs: int = 60
    scale_up_step: int = 2
    scale_down_step: int = 1
    cpu_band_pct: float = 10.0     # ± tolerance around target
    rps_band_pct: float = 15.0
    enabled: bool = True
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class MetricSnapshot:
    service_name: str
    cpu_pct: float
    rps: float
    replicas: int
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    memory_pct: float = 0.0
    latency_ms: float = 0.0


@dataclass
class ScalingDecision:
    service_name: str
    action: str
    current_replicas: int
    new_replicas: int
    reason: str
    cpu_pct: float = 0.0
    rps: float = 0.0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    applied: bool = False


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def _get_db(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scaling_policies (
            id              TEXT PRIMARY KEY,
            service_name    TEXT NOT NULL UNIQUE,
            min_replicas    INTEGER NOT NULL DEFAULT 1,
            max_replicas    INTEGER NOT NULL DEFAULT 10,
            target_cpu_pct  REAL NOT NULL DEFAULT 70.0,
            target_rps      REAL NOT NULL DEFAULT 100.0,
            cooldown_secs   INTEGER NOT NULL DEFAULT 60,
            scale_up_step   INTEGER NOT NULL DEFAULT 2,
            scale_down_step INTEGER NOT NULL DEFAULT 1,
            cpu_band_pct    REAL NOT NULL DEFAULT 10.0,
            rps_band_pct    REAL NOT NULL DEFAULT 15.0,
            enabled         INTEGER NOT NULL DEFAULT 1,
            created_at      REAL NOT NULL,
            updated_at      REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metric_snapshots (
            id           TEXT PRIMARY KEY,
            service_name TEXT NOT NULL,
            cpu_pct      REAL NOT NULL,
            rps          REAL NOT NULL,
            replicas     INTEGER NOT NULL,
            memory_pct   REAL NOT NULL DEFAULT 0,
            latency_ms   REAL NOT NULL DEFAULT 0,
            timestamp    REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS scaling_history (
            id               TEXT PRIMARY KEY,
            service_name     TEXT NOT NULL,
            action           TEXT NOT NULL,
            current_replicas INTEGER NOT NULL,
            new_replicas     INTEGER NOT NULL,
            reason           TEXT NOT NULL,
            cpu_pct          REAL NOT NULL DEFAULT 0,
            rps              REAL NOT NULL DEFAULT 0,
            applied          INTEGER NOT NULL DEFAULT 0,
            timestamp        REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS replica_state (
            service_name TEXT PRIMARY KEY,
            replicas     INTEGER NOT NULL DEFAULT 1,
            last_scale_ts REAL NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_metrics_svc ON metric_snapshots(service_name);
        CREATE INDEX IF NOT EXISTS idx_metrics_ts  ON metric_snapshots(timestamp);
        CREATE INDEX IF NOT EXISTS idx_history_svc ON scaling_history(service_name);
    """)
    conn.commit()


def _row_to_policy(row: sqlite3.Row) -> ScalingPolicy:
    return ScalingPolicy(
        id=row["id"],
        service_name=row["service_name"],
        min_replicas=row["min_replicas"],
        max_replicas=row["max_replicas"],
        target_cpu_pct=row["target_cpu_pct"],
        target_rps=row["target_rps"],
        cooldown_secs=row["cooldown_secs"],
        scale_up_step=row["scale_up_step"],
        scale_down_step=row["scale_down_step"],
        cpu_band_pct=row["cpu_band_pct"],
        rps_band_pct=row["rps_band_pct"],
        enabled=bool(row["enabled"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ---------------------------------------------------------------------------
# Policy management
# ---------------------------------------------------------------------------

def add_policy(
    service_name: str,
    min_replicas: int,
    max_replicas: int,
    target_cpu_pct: float,
    target_rps: float,
    cooldown_secs: int = 60,
    scale_up_step: int = 2,
    scale_down_step: int = 1,
    db: Optional[sqlite3.Connection] = None,
) -> ScalingPolicy:
    """Add or replace a scaling policy for a service."""
    if min_replicas < 1:
        raise ValueError("min_replicas must be >= 1")
    if max_replicas < min_replicas:
        raise ValueError("max_replicas must be >= min_replicas")
    if not (0 < target_cpu_pct <= 100):
        raise ValueError("target_cpu_pct must be in (0, 100]")
    if target_rps <= 0:
        raise ValueError("target_rps must be > 0")

    conn = db or _get_db()
    now = time.time()
    p = ScalingPolicy(
        service_name=service_name,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        target_cpu_pct=target_cpu_pct,
        target_rps=target_rps,
        cooldown_secs=cooldown_secs,
        scale_up_step=scale_up_step,
        scale_down_step=scale_down_step,
        created_at=now,
        updated_at=now,
    )
    conn.execute(
        """INSERT OR REPLACE INTO scaling_policies
           (id,service_name,min_replicas,max_replicas,target_cpu_pct,target_rps,
            cooldown_secs,scale_up_step,scale_down_step,cpu_band_pct,rps_band_pct,
            enabled,created_at,updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (p.id, p.service_name, p.min_replicas, p.max_replicas, p.target_cpu_pct,
         p.target_rps, p.cooldown_secs, p.scale_up_step, p.scale_down_step,
         p.cpu_band_pct, p.rps_band_pct, int(p.enabled), p.created_at, p.updated_at),
    )
    # Initialise replica state if not present
    conn.execute(
        "INSERT OR IGNORE INTO replica_state (service_name,replicas,last_scale_ts) VALUES (?,?,0)",
        (service_name, min_replicas),
    )
    conn.commit()
    return p


def get_policy(service_name: str, db: Optional[sqlite3.Connection] = None) -> Optional[ScalingPolicy]:
    conn = db or _get_db()
    row = conn.execute("SELECT * FROM scaling_policies WHERE service_name=?", (service_name,)).fetchone()
    return _row_to_policy(row) if row else None


def remove_policy(service_name: str, db: Optional[sqlite3.Connection] = None) -> bool:
    conn = db or _get_db()
    cur = conn.execute("DELETE FROM scaling_policies WHERE service_name=?", (service_name,))
    conn.commit()
    return cur.rowcount > 0


def list_policies(db: Optional[sqlite3.Connection] = None) -> list[ScalingPolicy]:
    conn = db or _get_db()
    return [_row_to_policy(r) for r in conn.execute("SELECT * FROM scaling_policies").fetchall()]


# ---------------------------------------------------------------------------
# Metric ingestion
# ---------------------------------------------------------------------------

def ingest_metric(
    service_name: str,
    cpu_pct: float,
    rps: float,
    replicas: int,
    memory_pct: float = 0.0,
    latency_ms: float = 0.0,
    db: Optional[sqlite3.Connection] = None,
) -> MetricSnapshot:
    """Record a metric snapshot for a service."""
    conn = db or _get_db()
    snap = MetricSnapshot(
        service_name=service_name,
        cpu_pct=cpu_pct,
        rps=rps,
        replicas=replicas,
        memory_pct=memory_pct,
        latency_ms=latency_ms,
    )
    conn.execute(
        """INSERT INTO metric_snapshots
           (id,service_name,cpu_pct,rps,replicas,memory_pct,latency_ms,timestamp)
           VALUES (?,?,?,?,?,?,?,?)""",
        (snap.id, snap.service_name, snap.cpu_pct, snap.rps, snap.replicas,
         snap.memory_pct, snap.latency_ms, snap.timestamp),
    )
    # Keep only last 1000 snapshots per service to avoid unbounded growth
    conn.execute(
        """DELETE FROM metric_snapshots WHERE service_name=? AND id NOT IN (
               SELECT id FROM metric_snapshots WHERE service_name=?
               ORDER BY timestamp DESC LIMIT 1000)""",
        (service_name, service_name),
    )
    # Update replica state
    conn.execute(
        "INSERT OR REPLACE INTO replica_state (service_name,replicas,last_scale_ts) VALUES (?,?,?)",
        (service_name, replicas,
         conn.execute("SELECT last_scale_ts FROM replica_state WHERE service_name=?", (service_name,)).fetchone()["last_scale_ts"]
         if conn.execute("SELECT 1 FROM replica_state WHERE service_name=?", (service_name,)).fetchone()
         else 0),
    )
    conn.commit()
    return snap


def get_recent_metrics(
    service_name: str,
    window_secs: int = 300,
    db: Optional[sqlite3.Connection] = None,
) -> list[MetricSnapshot]:
    conn = db or _get_db()
    since = time.time() - window_secs
    rows = conn.execute(
        "SELECT * FROM metric_snapshots WHERE service_name=? AND timestamp>=? ORDER BY timestamp DESC",
        (service_name, since),
    ).fetchall()
    return [
        MetricSnapshot(
            id=r["id"],
            service_name=r["service_name"],
            cpu_pct=r["cpu_pct"],
            rps=r["rps"],
            replicas=r["replicas"],
            memory_pct=r["memory_pct"],
            latency_ms=r["latency_ms"],
            timestamp=r["timestamp"],
        )
        for r in rows
    ]


def _avg_metrics(snapshots: list[MetricSnapshot]) -> tuple[float, float, int]:
    """Return (avg_cpu, avg_rps, latest_replicas)."""
    if not snapshots:
        return 0.0, 0.0, 1
    cpu = sum(s.cpu_pct for s in snapshots) / len(snapshots)
    rps = sum(s.rps for s in snapshots) / len(snapshots)
    replicas = snapshots[0].replicas  # most recent
    return cpu, rps, replicas


# ---------------------------------------------------------------------------
# Scaling evaluation
# ---------------------------------------------------------------------------

def evaluate_scaling(
    service_name: str,
    window_secs: int = 300,
    db: Optional[sqlite3.Connection] = None,
) -> ScalingDecision:
    """
    Evaluate whether to scale up, down, or do nothing for service_name.
    Returns a ScalingDecision (not yet applied).
    """
    conn = db or _get_db()
    policy = get_policy(service_name, db=conn)
    if not policy or not policy.enabled:
        return ScalingDecision(service_name, ScaleAction.NONE.value, 0, 0, ScaleReason.NO_METRICS.value)

    snapshots = get_recent_metrics(service_name, window_secs=window_secs, db=conn)
    if not snapshots:
        return ScalingDecision(service_name, ScaleAction.NONE.value, 0, 0, ScaleReason.NO_METRICS.value)

    avg_cpu, avg_rps, current_replicas = _avg_metrics(snapshots)

    # Check cooldown
    rs_row = conn.execute("SELECT last_scale_ts FROM replica_state WHERE service_name=?", (service_name,)).fetchone()
    last_scale_ts = rs_row["last_scale_ts"] if rs_row else 0
    if time.time() - last_scale_ts < policy.cooldown_secs:
        return ScalingDecision(
            service_name, ScaleAction.NONE.value,
            current_replicas, current_replicas,
            ScaleReason.COOLDOWN.value, avg_cpu, avg_rps,
        )

    # Determine if scale-up is needed
    cpu_high = avg_cpu > policy.target_cpu_pct + policy.cpu_band_pct
    rps_high = avg_rps > policy.target_rps * (1 + policy.rps_band_pct / 100)
    cpu_low  = avg_cpu < policy.target_cpu_pct - policy.cpu_band_pct
    rps_low  = avg_rps < policy.target_rps * (1 - policy.rps_band_pct / 100)

    if cpu_high or rps_high:
        if current_replicas >= policy.max_replicas:
            return ScalingDecision(service_name, ScaleAction.NONE.value,
                                   current_replicas, current_replicas,
                                   ScaleReason.AT_MAX.value, avg_cpu, avg_rps)
        new_replicas = min(current_replicas + policy.scale_up_step, policy.max_replicas)
        reason = ScaleReason.HIGH_CPU.value if cpu_high else ScaleReason.HIGH_RPS.value
        return ScalingDecision(service_name, ScaleAction.SCALE_UP.value,
                               current_replicas, new_replicas, reason, avg_cpu, avg_rps)

    if cpu_low and rps_low:
        if current_replicas <= policy.min_replicas:
            return ScalingDecision(service_name, ScaleAction.NONE.value,
                                   current_replicas, current_replicas,
                                   ScaleReason.AT_MIN.value, avg_cpu, avg_rps)
        new_replicas = max(current_replicas - policy.scale_down_step, policy.min_replicas)
        return ScalingDecision(service_name, ScaleAction.SCALE_DOWN.value,
                               current_replicas, new_replicas,
                               ScaleReason.LOW_CPU.value, avg_cpu, avg_rps)

    return ScalingDecision(service_name, ScaleAction.NONE.value,
                           current_replicas, current_replicas,
                           ScaleReason.WITHIN_BAND.value, avg_cpu, avg_rps)


def apply_scaling(
    service_name: str,
    new_replicas: int,
    db: Optional[sqlite3.Connection] = None,
) -> dict:
    """Apply a scaling decision: update replica state and log to history."""
    conn = db or _get_db()
    decision = evaluate_scaling(service_name, db=conn)
    now = time.time()

    # Persist the replica count change
    conn.execute(
        "INSERT OR REPLACE INTO replica_state (service_name,replicas,last_scale_ts) VALUES (?,?,?)",
        (service_name, new_replicas, now),
    )
    # Record in history
    conn.execute(
        """INSERT INTO scaling_history
           (id,service_name,action,current_replicas,new_replicas,reason,cpu_pct,rps,applied,timestamp)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (str(uuid.uuid4()), service_name, decision.action, decision.current_replicas,
         new_replicas, decision.reason, decision.cpu_pct, decision.rps, 1, now),
    )
    conn.commit()
    return {
        "service": service_name,
        "old_replicas": decision.current_replicas,
        "new_replicas": new_replicas,
        "action": decision.action,
        "reason": decision.reason,
        "applied_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
    }


def evaluate_and_apply(service_name: str, db: Optional[sqlite3.Connection] = None) -> dict:
    """Convenience: evaluate scaling and apply if action != none."""
    conn = db or _get_db()
    decision = evaluate_scaling(service_name, db=conn)
    if decision.action == ScaleAction.NONE.value:
        return {
            "service": service_name,
            "action": "none",
            "reason": decision.reason,
            "replicas": decision.current_replicas,
        }
    return apply_scaling(service_name, decision.new_replicas, db=conn)


# ---------------------------------------------------------------------------
# History & stats
# ---------------------------------------------------------------------------

def get_scaling_history(
    service_name: str,
    limit: int = 50,
    db: Optional[sqlite3.Connection] = None,
) -> list[dict]:
    """Return recent scaling decisions for a service."""
    conn = db or _get_db()
    rows = conn.execute(
        """SELECT * FROM scaling_history WHERE service_name=?
           ORDER BY timestamp DESC LIMIT ?""",
        (service_name, limit),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "action": r["action"],
            "current_replicas": r["current_replicas"],
            "new_replicas": r["new_replicas"],
            "reason": r["reason"],
            "cpu_pct": r["cpu_pct"],
            "rps": r["rps"],
            "applied": bool(r["applied"]),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(r["timestamp"])),
        }
        for r in rows
    ]


def get_replica_state(service_name: str, db: Optional[sqlite3.Connection] = None) -> dict:
    conn = db or _get_db()
    row = conn.execute("SELECT * FROM replica_state WHERE service_name=?", (service_name,)).fetchone()
    if not row:
        return {"service": service_name, "replicas": None}
    return {
        "service": service_name,
        "replicas": row["replicas"],
        "last_scale_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(row["last_scale_ts"])) if row["last_scale_ts"] else None,
    }


def get_scaling_summary(db: Optional[sqlite3.Connection] = None) -> list[dict]:
    """Return scaling summary for all services."""
    conn = db or _get_db()
    policies = list_policies(db=conn)
    summary = []
    for p in policies:
        rs = get_replica_state(p.service_name, db=conn)
        snaps = get_recent_metrics(p.service_name, window_secs=300, db=conn)
        avg_cpu, avg_rps, _ = _avg_metrics(snaps)
        summary.append({
            "service": p.service_name,
            "replicas": rs.get("replicas", p.min_replicas),
            "min_replicas": p.min_replicas,
            "max_replicas": p.max_replicas,
            "target_cpu": p.target_cpu_pct,
            "target_rps": p.target_rps,
            "avg_cpu_5m": round(avg_cpu, 1),
            "avg_rps_5m": round(avg_rps, 1),
            "enabled": p.enabled,
        })
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli_main() -> None:
    import argparse, sys

    p = argparse.ArgumentParser(prog="auto_scaler", description="BlackRoad Auto Scaler")
    sub = p.add_subparsers(dest="cmd")

    add = sub.add_parser("add-policy", help="Add/update scaling policy")
    add.add_argument("service")
    add.add_argument("--min", type=int, default=1, dest="min_replicas")
    add.add_argument("--max", type=int, default=10, dest="max_replicas")
    add.add_argument("--target-cpu", type=float, default=70.0)
    add.add_argument("--target-rps", type=float, default=100.0)
    add.add_argument("--cooldown", type=int, default=60)

    ingest = sub.add_parser("ingest", help="Ingest a metric snapshot")
    ingest.add_argument("service")
    ingest.add_argument("--cpu", type=float, required=True)
    ingest.add_argument("--rps", type=float, required=True)
    ingest.add_argument("--replicas", type=int, required=True)

    ev = sub.add_parser("evaluate", help="Evaluate scaling for a service")
    ev.add_argument("service")

    ap = sub.add_parser("apply", help="Apply scaling decision")
    ap.add_argument("service")

    hist = sub.add_parser("history", help="Get scaling history")
    hist.add_argument("service")
    hist.add_argument("--limit", type=int, default=20)

    sub.add_parser("summary", help="Scaling summary for all services")

    args = p.parse_args()
    db = _get_db()

    if args.cmd == "add-policy":
        pol = add_policy(args.service, args.min_replicas, args.max_replicas,
                         args.target_cpu, args.target_rps, args.cooldown, db=db)
        print(json.dumps({"id": pol.id, "service": pol.service_name}, indent=2))
    elif args.cmd == "ingest":
        snap = ingest_metric(args.service, args.cpu, args.rps, args.replicas, db=db)
        print(json.dumps({"id": snap.id, "service": snap.service_name}, indent=2))
    elif args.cmd == "evaluate":
        d = evaluate_scaling(args.service, db=db)
        print(json.dumps(d.__dict__, indent=2, default=str))
    elif args.cmd == "apply":
        print(json.dumps(evaluate_and_apply(args.service, db=db), indent=2))
    elif args.cmd == "history":
        print(json.dumps(get_scaling_history(args.service, args.limit, db=db), indent=2))
    elif args.cmd == "summary":
        print(json.dumps(get_scaling_summary(db=db), indent=2))
    else:
        p.print_help()
        sys.exit(1)


if __name__ == "__main__":
    _cli_main()

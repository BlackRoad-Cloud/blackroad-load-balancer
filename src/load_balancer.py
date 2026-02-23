"""
BlackRoad Load Balancer
Production-quality load balancer with multiple algorithms, health checking,
connection tracking, and SQLite persistence.
"""

from __future__ import annotations
import argparse
import hashlib
import json
import math
import random
import sqlite3
import sys
import time
import threading
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


DB_PATH = Path.home() / ".blackroad" / "load_balancer.db"

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BackendServer:
    host: str
    port: int
    weight: int = 1
    health: bool = True
    active_conns: int = 0
    total_requests: int = 0
    failed_requests: int = 0
    last_checked: float = field(default_factory=time.time)
    response_times: list[float] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if not (1 <= self.port <= 65535):
            raise ValueError(f"Invalid port: {self.port}")
        if self.weight < 1:
            raise ValueError("weight must be >= 1")
        if not self.host:
            raise ValueError("host cannot be empty")

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def avg_response_ms(self) -> float:
        if not self.response_times:
            return 0.0
        return round(sum(self.response_times[-100:]) / len(self.response_times[-100:]), 2)

    @property
    def error_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return round(self.failed_requests / self.total_requests, 4)

    @property
    def p95_response_ms(self) -> float:
        times = sorted(self.response_times[-100:])
        if not times:
            return 0.0
        idx = math.ceil(len(times) * 0.95) - 1
        return round(times[max(0, idx)], 2)


# ---------------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------------

def _init_db(path: Path = DB_PATH) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS servers (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            host           TEXT NOT NULL,
            port           INTEGER NOT NULL,
            weight         INTEGER NOT NULL DEFAULT 1,
            health         INTEGER NOT NULL DEFAULT 1,
            active_conns   INTEGER NOT NULL DEFAULT 0,
            total_requests INTEGER NOT NULL DEFAULT 0,
            failed_requests INTEGER NOT NULL DEFAULT 0,
            last_checked   REAL,
            tags           TEXT NOT NULL DEFAULT '{}',
            added_at       REAL NOT NULL,
            UNIQUE(host, port)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS request_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            client_ip    TEXT,
            backend_host TEXT NOT NULL,
            backend_port INTEGER NOT NULL,
            algorithm    TEXT NOT NULL,
            latency_ms   REAL,
            success      INTEGER NOT NULL DEFAULT 1,
            path         TEXT,
            logged_at    REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS health_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            host        TEXT NOT NULL,
            port        INTEGER NOT NULL,
            healthy     INTEGER NOT NULL DEFAULT 1,
            latency_ms  REAL,
            checked_at  REAL NOT NULL,
            error       TEXT
        )
    """)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# LoadBalancer class
# ---------------------------------------------------------------------------

class LoadBalancer:
    """
    Multi-algorithm load balancer with health checking and persistence.
    Thread-safe via a reentrant lock.
    """

    def __init__(self, db_path: Path = DB_PATH):
        self._db_path = db_path
        self._db = _init_db(db_path)
        self._lock = threading.RLock()
        self._servers: list[BackendServer] = []
        self._rr_index: int = 0
        self._wrr_counters: dict[str, int] = {}
        self._load_from_db()

    def _load_from_db(self) -> None:
        rows = self._db.execute("SELECT * FROM servers ORDER BY id").fetchall()
        cols = [d[0] for d in self._db.execute("SELECT * FROM servers LIMIT 0").description]
        for row in rows:
            data = dict(zip(cols, row))
            self._servers.append(BackendServer(
                host=data["host"],
                port=data["port"],
                weight=data["weight"],
                health=bool(data["health"]),
                active_conns=data["active_conns"],
                total_requests=data["total_requests"],
                failed_requests=data["failed_requests"],
                last_checked=data.get("last_checked") or time.time(),
                tags=json.loads(data.get("tags", "{}")),
            ))

    def _persist_server(self, server: BackendServer) -> None:
        self._db.execute(
            """INSERT INTO servers (host,port,weight,health,active_conns,total_requests,
               failed_requests,last_checked,tags,added_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(host,port) DO UPDATE SET
                 weight=excluded.weight, health=excluded.health,
                 active_conns=excluded.active_conns, total_requests=excluded.total_requests,
                 failed_requests=excluded.failed_requests, last_checked=excluded.last_checked,
                 tags=excluded.tags""",
            (server.host, server.port, server.weight, int(server.health),
             server.active_conns, server.total_requests, server.failed_requests,
             server.last_checked, json.dumps(server.tags), time.time()),
        )
        self._db.commit()

    def add_server(self, server: BackendServer) -> bool:
        with self._lock:
            for s in self._servers:
                if s.host == server.host and s.port == server.port:
                    return False  # Already exists
            self._servers.append(server)
            self._persist_server(server)
            return True

    def remove_server(self, host: str, port: int) -> bool:
        with self._lock:
            before = len(self._servers)
            self._servers = [s for s in self._servers if not (s.host == host and s.port == port)]
            if len(self._servers) < before:
                self._db.execute("DELETE FROM servers WHERE host=? AND port=?", (host, port))
                self._db.commit()
                return True
            return False

    def _healthy_servers(self) -> list[BackendServer]:
        return [s for s in self._servers if s.health]

    # ------------------------------------------------------------------
    # Algorithms
    # ------------------------------------------------------------------

    def _round_robin(self) -> Optional[BackendServer]:
        servers = self._healthy_servers()
        if not servers:
            return None
        server = servers[self._rr_index % len(servers)]
        self._rr_index = (self._rr_index + 1) % len(servers)
        return server

    def _weighted_round_robin(self) -> Optional[BackendServer]:
        servers = self._healthy_servers()
        if not servers:
            return None
        weighted: list[BackendServer] = []
        for s in servers:
            weighted.extend([s] * s.weight)
        key = id(self)
        self._wrr_counters[str(key)] = self._wrr_counters.get(str(key), 0)
        idx = self._wrr_counters[str(key)] % len(weighted)
        self._wrr_counters[str(key)] = idx + 1
        return weighted[idx]

    def _least_connections(self) -> Optional[BackendServer]:
        servers = self._healthy_servers()
        if not servers:
            return None
        return min(servers, key=lambda s: s.active_conns)

    def _ip_hash(self, client_ip: str) -> Optional[BackendServer]:
        servers = self._healthy_servers()
        if not servers:
            return None
        h = int(hashlib.md5(client_ip.encode()).hexdigest(), 16)
        return servers[h % len(servers)]

    def _random(self) -> Optional[BackendServer]:
        servers = self._healthy_servers()
        if not servers:
            return None
        return random.choice(servers)

    def _least_response_time(self) -> Optional[BackendServer]:
        servers = self._healthy_servers()
        if not servers:
            return None
        return min(servers, key=lambda s: s.avg_response_ms if s.avg_response_ms > 0 else float("inf"))

    def get_next_server(
        self,
        algorithm: str = "round_robin",
        client_ip: Optional[str] = None,
    ) -> Optional[BackendServer]:
        """Select the next backend server using the given algorithm."""
        with self._lock:
            algo_map = {
                "round_robin": self._round_robin,
                "weighted_round_robin": self._weighted_round_robin,
                "least_connections": self._least_connections,
                "ip_hash": lambda: self._ip_hash(client_ip or "127.0.0.1"),
                "random": self._random,
                "least_response_time": self._least_response_time,
            }
            if algorithm not in algo_map:
                raise ValueError(f"Unknown algorithm: {algorithm}. "
                                 f"Available: {list(algo_map.keys())}")
            server = algo_map[algorithm]()
            if server:
                server.active_conns += 1
                server.total_requests += 1
            return server

    def release_connection(self, server: BackendServer, success: bool = True,
                           latency_ms: float = 0.0) -> None:
        with self._lock:
            server.active_conns = max(0, server.active_conns - 1)
            if not success:
                server.failed_requests += 1
            if latency_ms > 0:
                server.response_times.append(latency_ms)
                server.response_times = server.response_times[-200:]  # Keep last 200
            self._persist_server(server)

    # ------------------------------------------------------------------
    # Health checking
    # ------------------------------------------------------------------

    def health_check(self, server: BackendServer, timeout: float = 2.0) -> dict:
        url = f"http://{server.host}:{server.port}/health"
        start = time.time()
        healthy = False
        error = ""
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "blackroad-lb/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                healthy = resp.status == 200
        except urllib.error.URLError as e:
            error = str(e.reason)
        except Exception as e:
            error = str(e)

        latency_ms = (time.time() - start) * 1000
        with self._lock:
            server.health = healthy
            server.last_checked = time.time()
            self._persist_server(server)
            self._db.execute(
                "INSERT INTO health_log (host,port,healthy,latency_ms,checked_at,error) VALUES (?,?,?,?,?,?)",
                (server.host, server.port, int(healthy), round(latency_ms, 2), time.time(), error),
            )
            self._db.commit()
        return {"server": server.address, "healthy": healthy,
                "latency_ms": round(latency_ms, 2), "error": error}

    def health_check_all(self) -> list[dict]:
        return [self.health_check(s) for s in self._servers]

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats_report(self) -> dict:
        with self._lock:
            total_req = sum(s.total_requests for s in self._servers)
            total_fail = sum(s.failed_requests for s in self._servers)
            total_conns = sum(s.active_conns for s in self._servers)
            healthy = sum(1 for s in self._servers if s.health)
            return {
                "total_servers": len(self._servers),
                "healthy_servers": healthy,
                "unhealthy_servers": len(self._servers) - healthy,
                "total_requests": total_req,
                "total_failures": total_fail,
                "active_connections": total_conns,
                "global_error_rate": round(total_fail / total_req, 4) if total_req > 0 else 0,
                "servers": [
                    {
                        "address": s.address,
                        "weight": s.weight,
                        "health": s.health,
                        "active_conns": s.active_conns,
                        "total_requests": s.total_requests,
                        "error_rate": s.error_rate,
                        "avg_response_ms": s.avg_response_ms,
                        "p95_response_ms": s.p95_response_ms,
                    }
                    for s in self._servers
                ],
            }

    def simulate_requests(
        self, count: int = 100, algorithm: str = "round_robin",
        client_ip: str = "192.168.1.1"
    ) -> dict:
        """Simulate load balancing requests for testing."""
        distribution: dict[str, int] = {}
        for _ in range(count):
            srv = self.get_next_server(algorithm, client_ip)
            if srv:
                latency = random.uniform(5, 200)
                success = random.random() > 0.05
                self.release_connection(srv, success, latency)
                key = srv.address
                distribution[key] = distribution.get(key, 0) + 1
        return {"count": count, "algorithm": algorithm, "distribution": distribution}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cmd_add(args: argparse.Namespace) -> None:
    lb = LoadBalancer()
    server = BackendServer(host=args.host, port=args.port, weight=args.weight)
    if lb.add_server(server):
        print(f"Added server {server.address} (weight={args.weight})")
    else:
        print(f"Server {server.address} already exists")


def _cmd_remove(args: argparse.Namespace) -> None:
    lb = LoadBalancer()
    if lb.remove_server(args.host, args.port):
        print(f"Removed server {args.host}:{args.port}")
    else:
        print(f"Server {args.host}:{args.port} not found", file=sys.stderr)
        sys.exit(1)


def _cmd_list(args: argparse.Namespace) -> None:
    lb = LoadBalancer()
    if not lb._servers:
        print("No servers configured.")
        return
    print(f"{'ADDRESS':<25} {'WEIGHT':<8} {'HEALTH':<8} {'CONNS':<7} {'REQUESTS':<10} {'ERR%'}")
    print("-" * 70)
    for s in lb._servers:
        health = "UP" if s.health else "DOWN"
        err_pct = f"{s.error_rate * 100:.1f}%"
        print(f"{s.address:<25} {s.weight:<8} {health:<8} {s.active_conns:<7} {s.total_requests:<10} {err_pct}")


def _cmd_balance(args: argparse.Namespace) -> None:
    lb = LoadBalancer()
    server = lb.get_next_server(args.algorithm, args.client_ip)
    if server:
        print(f"Routed to: {server.address} (algorithm={args.algorithm})")
        lb.release_connection(server, latency_ms=random.uniform(5, 50))
    else:
        print("No healthy servers available", file=sys.stderr)
        sys.exit(1)


def _cmd_health(args: argparse.Namespace) -> None:
    lb = LoadBalancer()
    results = lb.health_check_all()
    for r in results:
        icon = "✅" if r["healthy"] else "❌"
        print(f"{icon} {r['server']}: {r['latency_ms']:.1f}ms {r['error']}")


def _cmd_stats(args: argparse.Namespace) -> None:
    lb = LoadBalancer()
    report = lb.stats_report()
    print(json.dumps(report, indent=2))


def _cmd_simulate(args: argparse.Namespace) -> None:
    lb = LoadBalancer()
    result = lb.simulate_requests(args.count, args.algorithm, args.client_ip or "10.0.0.1")
    print(f"Simulated {result['count']} requests using {result['algorithm']}")
    print("\nDistribution:")
    total = sum(result["distribution"].values())
    for addr, count in sorted(result["distribution"].items(), key=lambda x: -x[1]):
        pct = count / total * 100
        bar = "█" * int(pct / 2)
        print(f"  {addr:<25} {count:>5} ({pct:5.1f}%) {bar}")


def build_parser() -> argparse.ArgumentParser:
    algos = ["round_robin", "weighted_round_robin", "least_connections",
             "ip_hash", "random", "least_response_time"]
    parser = argparse.ArgumentParser(description="BlackRoad Load Balancer")
    sub = parser.add_subparsers(dest="command")

    add = sub.add_parser("add", help="Add a backend server")
    add.add_argument("host")
    add.add_argument("port", type=int)
    add.add_argument("--weight", type=int, default=1)

    rm = sub.add_parser("remove", help="Remove a backend server")
    rm.add_argument("host")
    rm.add_argument("port", type=int)

    sub.add_parser("list", help="List backend servers")

    bal = sub.add_parser("balance", help="Get next server for a request")
    bal.add_argument("--algorithm", default="round_robin", choices=algos)
    bal.add_argument("--client-ip", default=None)

    sub.add_parser("health", help="Health check all servers")
    sub.add_parser("stats", help="Show statistics report")

    sim = sub.add_parser("simulate", help="Simulate requests")
    sim.add_argument("--count", type=int, default=100)
    sim.add_argument("--algorithm", default="round_robin", choices=algos)
    sim.add_argument("--client-ip", default=None)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    dispatch = {
        "add": _cmd_add, "remove": _cmd_remove, "list": _cmd_list,
        "balance": _cmd_balance, "health": _cmd_health, "stats": _cmd_stats,
        "simulate": _cmd_simulate,
    }
    if args.command in dispatch:
        dispatch[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()

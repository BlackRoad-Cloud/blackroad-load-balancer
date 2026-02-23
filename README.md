# BlackRoad Load Balancer

> Multi-algorithm load balancer with health checking, P95 latency tracking, and thread-safe connection management.

## Features

- `BackendServer` — host, port, weight, health, active_conns, response_times
- P95 latency and error rate per server
- **Algorithms**: `round_robin`, `weighted_round_robin`, `least_connections`, `ip_hash`, `random`, `least_response_time`
- `add_server()`, `remove_server()` — dynamic backend management
- `get_next_server(algorithm, client_ip)` — thread-safe selection
- `health_check()` / `health_check_all()` — HTTP health probes
- `stats_report()` — global and per-server statistics
- `simulate_requests()` — load distribution testing
- SQLite persistence with WAL mode
- CLI: `add`, `remove`, `list`, `balance`, `health`, `stats`, `simulate`

## Usage

```bash
# Add backend servers
python src/load_balancer.py add 10.0.0.1 8080 --weight 2
python src/load_balancer.py add 10.0.0.2 8080 --weight 1
python src/load_balancer.py add 10.0.0.3 8080 --weight 3

# Get next server (various algorithms)
python src/load_balancer.py balance --algorithm round_robin
python src/load_balancer.py balance --algorithm least_connections
python src/load_balancer.py balance --algorithm ip_hash --client-ip 192.168.1.50

# Health check all backends
python src/load_balancer.py health

# View stats
python src/load_balancer.py stats

# Simulate 1000 requests
python src/load_balancer.py simulate --count 1000 --algorithm weighted_round_robin
```

## Algorithm Summary

| Algorithm | Best For |
|-----------|----------|
| `round_robin` | Equal-capacity backends |
| `weighted_round_robin` | Mixed-capacity backends |
| `least_connections` | Long-lived connections (WebSocket, gRPC) |
| `ip_hash` | Session affinity (sticky sessions) |
| `random` | Simple stateless services |
| `least_response_time` | Latency-sensitive workloads |

## Tests

```bash
pytest tests/ -v --cov=src
```

<!-- BlackRoad SEO Enhanced -->

# ulackroad load ualancer

> Part of **[BlackRoad OS](https://blackroad.io)** — Sovereign Computing for Everyone

[![BlackRoad OS](https://img.shields.io/badge/BlackRoad-OS-ff1d6c?style=for-the-badge)](https://blackroad.io)
[![BlackRoad Cloud](https://img.shields.io/badge/Org-BlackRoad-Cloud-2979ff?style=for-the-badge)](https://github.com/BlackRoad-Cloud)
[![License](https://img.shields.io/badge/License-Proprietary-f5a623?style=for-the-badge)](LICENSE)

**ulackroad load ualancer** is part of the **BlackRoad OS** ecosystem — a sovereign, distributed operating system built on edge computing, local AI, and mesh networking by **BlackRoad OS, Inc.**

## About BlackRoad OS

BlackRoad OS is a sovereign computing platform that runs AI locally on your own hardware. No cloud dependencies. No API keys. No surveillance. Built by [BlackRoad OS, Inc.](https://github.com/BlackRoad-OS-Inc), a Delaware C-Corp founded in 2025.

### Key Features
- **Local AI** — Run LLMs on Raspberry Pi, Hailo-8, and commodity hardware
- **Mesh Networking** — WireGuard VPN, NATS pub/sub, peer-to-peer communication
- **Edge Computing** — 52 TOPS of AI acceleration across a Pi fleet
- **Self-Hosted Everything** — Git, DNS, storage, CI/CD, chat — all sovereign
- **Zero Cloud Dependencies** — Your data stays on your hardware

### The BlackRoad Ecosystem
| Organization | Focus |
|---|---|
| [BlackRoad OS](https://github.com/BlackRoad-OS) | Core platform and applications |
| [BlackRoad OS, Inc.](https://github.com/BlackRoad-OS-Inc) | Corporate and enterprise |
| [BlackRoad AI](https://github.com/BlackRoad-AI) | Artificial intelligence and ML |
| [BlackRoad Hardware](https://github.com/BlackRoad-Hardware) | Edge hardware and IoT |
| [BlackRoad Security](https://github.com/BlackRoad-Security) | Cybersecurity and auditing |
| [BlackRoad Quantum](https://github.com/BlackRoad-Quantum) | Quantum computing research |
| [BlackRoad Agents](https://github.com/BlackRoad-Agents) | Autonomous AI agents |
| [BlackRoad Network](https://github.com/BlackRoad-Network) | Mesh and distributed networking |
| [BlackRoad Education](https://github.com/BlackRoad-Education) | Learning and tutoring platforms |
| [BlackRoad Labs](https://github.com/BlackRoad-Labs) | Research and experiments |
| [BlackRoad Cloud](https://github.com/BlackRoad-Cloud) | Self-hosted cloud infrastructure |
| [BlackRoad Forge](https://github.com/BlackRoad-Forge) | Developer tools and utilities |

### Links
- **Website**: [blackroad.io](https://blackroad.io)
- **Documentation**: [docs.blackroad.io](https://docs.blackroad.io)
- **Chat**: [chat.blackroad.io](https://chat.blackroad.io)
- **Search**: [search.blackroad.io](https://search.blackroad.io)

---


> BlackRoad Cloud Infrastructure: blackroad-load-balancer

Part of the [BlackRoad OS](https://blackroad.io) ecosystem — [BlackRoad-Cloud](https://github.com/BlackRoad-Cloud)

---

# blackroad-auto-scaler

> Horizontal auto-scaler with CPU/RPS-based policies, cooldown enforcement, multi-metric ingestion, and full scaling history. SQLite backed.

## Features

- **Dual signal scaling** — scale on CPU% and/or RPS independently
- **Configurable bands** — tolerance window to prevent oscillation
- **Cooldown enforcement** — per-policy cooldown between scaling events
- **Step-based scaling** — configurable scale_up_step and scale_down_step
- **Metric rolling window** — evaluate over configurable time window (default 5 min)
- **Scaling history** — every decision recorded with reason and metrics
- **Summary dashboard** — current state of all services at a glance
- **SQLite persistence** — `~/.blackroad/auto_scaler.db`

## Quick start

```bash
pip install -r requirements.txt

# Add policy
python src/auto_scaler.py add-policy api \
  --min 2 --max 20 --target-cpu 70 --target-rps 100 --cooldown 60

# Ingest metrics
python src/auto_scaler.py ingest api --cpu 92.5 --rps 350 --replicas 4

# Evaluate & apply
python src/auto_scaler.py apply api

# History
python src/auto_scaler.py history api

# Dashboard
python src/auto_scaler.py summary
```

## API

```python
from src.auto_scaler import add_policy, ingest_metric, evaluate_scaling, apply_scaling, evaluate_and_apply

# Configure policy
add_policy("api", min_replicas=2, max_replicas=20,
           target_cpu_pct=70.0, target_rps=100.0, cooldown_secs=60)

# Feed metrics
ingest_metric("api", cpu_pct=92.5, rps=350.0, replicas=4)

# Evaluate (non-destructive)
decision = evaluate_scaling("api")
print(decision.action, decision.new_replicas, decision.reason)

# Apply
result = evaluate_and_apply("api")
# {"service": "api", "old_replicas": 4, "new_replicas": 6, "action": "scale_up"}

# History
history = get_scaling_history("api", limit=20)
```

## Scale logic

```
if avg_cpu > target_cpu + band OR avg_rps > target_rps * (1 + band):
    scale_up by scale_up_step (capped at max_replicas)
elif avg_cpu < target_cpu - band AND avg_rps < target_rps * (1 - band):
    scale_down by scale_down_step (floored at min_replicas)
else:
    no action
```

Cooldown and min/max bounds are always respected.

## Testing

```bash
pytest tests/ -v
```

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

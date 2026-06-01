# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

VVP3 Autopilot showcase: a realistic Orders-processing pipeline designed to trigger and demonstrate Autopilot scale-up/scale-down behaviour on Ververica Platform 3. See `PROJECT_PLAN.md` for the full staged plan and task breakdown.

**Stack**: Apache Kafka 3.7 (KRaft, 3-node StatefulSet, `kafka` namespace) → Flink 1.20 job on VVP3 → `order-metrics` Kafka topic → Consumer logger. Grafana + Prometheus dashboard for real-time Autopilot visibility.

**Key topics**: `orders` (12 partitions), `order-metrics` (6 partitions)  
**Flink state key**: `category + "#" + region` (ValueState, grows with throughput — primary Autopilot signal)  
**Peak simulation**: producer REST API at `POST /api/peak/start` — 10x multiplier, 5 min, configurable

## MCP Server: vvp3

An MCP server named `vvp3` is available in this session. It provides tools to interact with a live Ververica Platform 3 instance — managing deployments, session clusters, artifacts, resource queues, secrets, drafts, logs, catalogs, and task managers. Before using VVP3 tools, check the current context:

```
mcp__vvp3__current_context   # see active server/workspace/context
mcp__vvp3__list_workspaces   # list available workspaces
mcp__vvp3__list_deployments  # list Flink deployments
```

Key VVP3 workflows:
- **Deploy a JAR job**: `create_artifact` → `create_jar_deployment` → `start_deployment`
- **Deploy SQL**: `create_sql_deployment` → `start_deployment`
- **Monitor**: `get_deployment`, `get_job`, `get_jobmanager_log`, `list_task_managers`, `get_taskmanager_log`
- **Autoscaler config**: managed via deployment spec in `create_jar_deployment` / `create_python_deployment`

## Specialized Agents

Three agents are configured in `.claude/agents/` and are invoked automatically by Claude Code when the task matches their domain:

| Agent | Trigger |
|---|---|
| `flink-streaming-expert` | Flink DataStream/Table/SQL API, state management, windowing, connectors, checkpointing, backpressure |
| `kafka-client-developer` | Kafka producer/consumer code, serialization, consumer groups, delivery semantics |
| `k8s-kafka-admin` | Kubernetes manifests, Kafka on K8s (Strimzi/Confluent Operator), broker tuning, RBAC |

All three agents have project-scoped persistent memory under `.claude/agent-memory/<agent-name>/`.

## Key Integration Points

- **Kafka → Flink → VVP3**: Consumer lag from Kafka topics drives scaling decisions; the autoscaler adjusts parallelism or replica counts via the VVP3 API.
- **Flink version**: Confirm with the user before writing Flink code — APIs differ significantly between 1.x and 2.x.
- **Deployment language**: Java is the default for Flink jobs unless specified otherwise.

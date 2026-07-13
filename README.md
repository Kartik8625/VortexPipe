# VortexPipe — Real-Time Clickstream Analytics Pipeline

A Kafka-based ingestion and windowed-aggregation pipeline that answers "what's trending right now" via a live API — the same pattern behind real-time trending/leaderboard dashboards at e-commerce and media platforms, without relying on nightly batch jobs.

## Architecture

![Architecture](architecture-diagram.png)

```
Producer (event simulator)
      │
      ▼
   Kafka  ── 3-partition topic "clickstream"
      │
      ├──────────────────────────┐
      ▼                          ▼
Consumer Group (2 workers)   S3 Archiver (cold path)
      │                          │
      ├─→ Postgres (raw events)  └─→ Parquet batches → S3 (data lake)
      └─→ Redis ZSETs (1-min tumbling windows)
                │
                ▼
         FastAPI serving layer
                │
                ▼
            Client
```

**Three paths, one topic:**
- **Hot path** — Redis sorted sets maintain live 1-minute tumbling-window leaderboards, read by the API with sub-10ms latency.
- **Warm path** — Postgres stores validated raw events for historical/ad-hoc querying.
- **Cold path** — a standalone consumer micro-batches events into Parquet (via pandas/pyarrow) and writes to S3 for cheap long-term storage and downstream ML/analytics workloads.

## Design decisions (and their tradeoffs)

| Decision | Why | Tradeoff accepted |
|---|---|---|
| At-least-once delivery (offset commit after DB write) | Simpler, no distributed transaction coordination | Possible duplicate rows on consumer crash mid-write — acceptable for click analytics, not for billing-grade data |
| 3 Kafka partitions | Matches expected consumer parallelism (2–3 workers) without over-partitioning | Ceiling on horizontal scale without repartitioning |
| Redis ZSET tumbling windows (not Flink/ksqlDB) | Sufficient accuracy at this scale, near-zero infra overhead | No exactly-once windowing, no watermarking/late-event handling |
| Single Kafka broker | Free-tier/cost-constrained deployment | No partition replication — broker failure loses in-flight data; production would need ≥3 brokers with `replication.factor=3` |
| IAM role on EC2 (no hardcoded credentials) | Standard AWS security practice | N/A |

## Fault-tolerance tests performed

- **Consumer crash:** killed a running worker (`pkill`) mid-stream — Kafka triggered a consumer-group rebalance, orphaned partitions were reassigned to the surviving worker, and processing continued (at-least-once, so duplicates are possible across the crash boundary — not verified as zero-duplicate).
- **IAM permission revocation:** revoked S3 write access on the live archiver mid-run — writes failed with `AccessDenied`, the archiver caught the exception and paused rather than crashing or silently dropping data; recovered automatically once permissions were restored. (This is graceful degradation under backpressure, not a formal exactly-once guarantee.)
- **Malformed input ("poison pills"):** injected ~2% malformed events (bad timestamps/missing fields) into the stream — all were caught by schema validation and routed to a rejection counter instead of crashing the consumer or corrupting the DB.

## Measured performance

| Metric | Result | Notes |
|---|---|---|
| Sustained ingestion | 150 events/sec | Continuous producer load over WAN (local producer → cloud broker) |
| Peak throughput | 300 events/sec | Burst load, no dropped connections |
| Rows committed to Postgres | 2,859 | Validated events over test run |
| Parquet files written to S3 | 91 | Micro-batched at 200 events/file |
| Peak single-URL aggregation | 750+ clicks/min | `/about` under spike load, read from Redis ZSET |
| Malformed-input catch rate | 100% of injected 2% | All poison-pill events isolated, zero reached Postgres |

*Numbers from a single test run on an `m7i-flex.large` EC2 instance (Mumbai region); not a sustained production benchmark.*

## Tech stack

Kafka (KRaft mode) · Python (producer/consumer) · Redis (ZSET tumbling windows) · PostgreSQL · FastAPI · AWS EC2 · AWS S3 · AWS IAM · pandas/pyarrow (Parquet) · Docker Compose

## Setup

```bash
git clone https://github.com/<your-username>/vortexpipe.git
cd vortexpipe
docker compose up -d

# run producer
python producer/produce.py

# run 2 consumer workers (separate terminals — demonstrates partition rebalancing)
python consumer/consume.py
python consumer/consume.py

# run S3 cold-path archiver
python consumer/s3_archiver.py

# serving API
uvicorn api.main:app --reload
```

## API

```bash
GET /analytics/top-urls?window=current   # live leaderboard, current minute
GET /analytics/top-urls?window=last      # previous completed minute
GET /analytics/stats                     # total processed, rejected, rejection rate
```

## What I'd add at production scale

- **Kafka replication factor 3** across ≥3 brokers — current single-broker setup has no fault tolerance at the broker level.
- **Exactly-once semantics** via Kafka transactions if this fed billing/financial reporting instead of engagement analytics.
- **Flink or ksqlDB** for true event-time windowing with watermarks and late-event handling, replacing the Redis-approximated tumbling windows.
- **Terraform** for the EC2/Security Group/IAM provisioning instead of manual console setup.
- **Prometheus + Grafana** instead of the custom `/analytics/stats` endpoint for proper observability and alerting.

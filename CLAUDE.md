# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A hands-on Saga Orchestration Pattern implementation across 4 Spring Boot microservices (Order, Payment, Inventory, Saga Orchestrator), using Kafka for async choreography and gRPC for synchronous lookups. It's a system-design study project, not production code — optimize explanations and changes for clarity of the pattern over production hardening.

The core problem it solves: across independent services with separate databases, there's no single transaction spanning "charge the customer" and "confirm the order." The saga charges payment **last** (after order + stock succeed) so a decline never has to unwind a completed charge. The remaining gap — orchestrator crashing between "payment approved" and "order confirmed" — is closed by the transactional outbox, idempotency keys, and stuck-saga recovery described below. Read the root `README.md` for full architecture diagrams, the saga flow (happy path + both compensation scenarios), and the Kafka topic table — it's kept current and is the best first read for any saga-flow change.

## Repository layout

Single-folder Maven reactor at the repo root — five modules: `shared` + the four services (`payment-service`, `order-service`, `inventory-service`, `saga-orchestrator`). The root `pom.xml` only aggregates them for build ordering and IDE import; each service still inherits `spring-boot-starter-parent` directly and depends on `shared` (gRPC contracts + Kafka event classes) as a regular Maven dependency. There's a leftover empty `server/` directory (just `.idea` cache) from a prior layout — ignore it, all real modules live at the repo root now.

Every service follows the same internal package layout under `com.saga.<service>`:
- `entity` / `entity/enums` — JPA entities and status enums (each service also has its own `OutboxEvent` entity)
- `repository` — Spring Data JPA repositories
- `service` — core business logic
- `controller` — REST endpoints + `GlobalExceptionHandler`
- `grpc` — gRPC service impl and/or clients to other services
- `listener` — Kafka `@KafkaListener` consumers
- `outbox` — `OutboxEventProcessor`/`OutboxPublisher` (transactional outbox poller)
- `config` — `KafkaConfig`, etc.
- `dto` — request/response DTOs

`saga-orchestrator` additionally has `recovery/SagaRecoveryScheduler.java` (stuck-saga recovery) and `model/SagaPayload.java`. `shared/src/main/proto/*.proto` defines the gRPC contracts (`payment.proto`, `order.proto`, `inventory.proto`, `orchestrator.proto`) shared across services.

## Build, run, test

```bash
# Build everything (shared is built first automatically, reactor resolves order)
mvn install

# Start infrastructure: Postgres x4, Kafka, Zookeeper, Kafka UI (:8090)
docker-compose up -d

# Run a single service (separate terminal per service)
cd <service-dir> && mvn spring-boot:run

# Run all tests for one module
cd <service-dir> && mvn test

# Run a single test class
cd <service-dir> && mvn test -Dtest=OrderServiceTest

# Build one module without reinstalling the whole reactor
cd <service-dir> && mvn compile
```

From IntelliJ: open the repo root — the reactor `pom.xml` auto-imports all five modules; run/debug `*Application` classes directly, no manual `shared` install needed.

Each service's test suite lives at `src/test/java/com/saga/<service>/service/*ServiceTest.java` — currently one test class per service (`OrderServiceTest`, `PaymentServiceTest`, `InventoryServiceTest`, `SagaOrchestrationServiceTest`) covering idempotency and recovery logic. No integration/Testcontainers suite yet.

Schema is managed via Liquibase changelogs at `<service>/src/main/resources/db/changelog/`, auto-applied on startup — add new changesets under `changes/` and register them in `db.changelog-master.xml` rather than editing applied changesets.

## Ports and databases

| Service | REST | gRPC | DB | DB Port |
|---|---|---|---|---|
| saga-orchestrator | 8084 | 9084 | `orchestrator_db` | 5435 |
| order-service | 8082 | 9082 | `order_db` | 5433 |
| payment-service | 8081 | 9081 | `payment_db` | 5436 |
| inventory-service | 8083 | 9083 | `inventory_db` | 5434 |

Kafka broker on 9092 (host) / 29092 (inter-container), Kafka UI on 8090. Services run locally and connect to these via `docker-compose.yml` at the repo root.

## Architecture conventions to preserve

- **Service isolation**: each service owns its DB exclusively; cross-service communication is only via Kafka events (async) or gRPC (sync lookups) — never direct DB access across services.
- **Transactional outbox**: any new state change that must publish a Kafka event should write the domain row and an outbox row in the same local transaction, not publish to Kafka directly from the write path. The `OutboxEventProcessor` polls with `FOR UPDATE SKIP LOCKED` and retries with backoff.
- **Idempotency**: writes on the order/payment/stock/saga paths look up by idempotency key first and replay on duplicate. The orchestrator derives per-step keys as `<sagaId>:<STEP_NAME>` (e.g. `PAYMENT_PROCESS`). New saga steps or endpoints that mutate state should follow this pattern.
- **Payment ordering is deliberate**: payment is always the last forward step in the saga specifically so failures never require unwinding a completed charge. `payment.cancel.request`/`payment.cancelled` exist end-to-end but are currently unused for this reason — don't "clean them up," they're reserved for a future step inserted after payment.
- **Amount contract**: `totalAmount = unitPrice x quantity` is calculated once by order-service and charged verbatim by payment-service — don't recompute it downstream.
- **Concurrency**: inventory uses pessimistic locking to prevent over-reservation; keep that when touching stock reservation logic.
- **Saga audit trail**: every saga step is persisted in `saga_steps`, and `recovery_attempts` on `SagaInstance` tracks recovery interventions — recovery/compensation changes should keep writing to both.
- Amounts >= 10,000 are auto-declined by the simulated payment gateway — this is the deliberate trigger for exercising the payment-failure compensation path on demand, not a bug.

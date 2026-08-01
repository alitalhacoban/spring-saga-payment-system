# Saga Pattern Payment System

A hands-on microservices project that implements the **Saga Orchestration Pattern** to coordinate a distributed order/payment/inventory transaction across four independent Spring Boot services, using Kafka for asynchronous choreography and gRPC for synchronous lookups. This is **not a production-ready application** — it is a system-design study project, built to work through a specific class of distributed-systems failure in real code rather than in theory.

---

## The Problem This Project Explores: "Payment Deducted, But Order Failed"

This is the scenario that motivates the whole project, and the reason a saga orchestrator, an outbox table, idempotency keys, and a recovery job all exist in this codebase instead of a single database transaction.

In a monolith, "charge the customer and confirm the order" is one ACID transaction — it either fully happens or fully rolls back. Split across independent services with their own databases, that guarantee disappears. There is no distributed transaction spanning Payment's database and Order's database. If the process coordinating the steps crashes, or a network call is lost, or a downstream service is briefly unavailable, you can land in a state where **money has moved but the order never completes** — or the reverse, stock is held but never released. Either outcome is a direct loss of trust and, in the payment case, of money.

This project's saga deliberately orders its steps to shrink that risk window as much as possible: an order is created in `PENDING` state, then stock is reserved, and **payment is charged last**, only after both of those succeed. That ordering means a plain "gateway declined" failure never has to unwind a completed payment — it only has to release stock and cancel the order (see Compensation Path 2 below). Step ordering alone isn't enough, though — it doesn't help if the orchestrator itself crashes *after* the payment gateway approves the charge but *before* the order is confirmed. That gap is closed by three mechanisms working together, all present in this codebase:

- **Transactional outbox** — the payment result and the "publish this event" intent are written to the database in the same local transaction, so a crash between "charge succeeded" and "notify the rest of the saga" is impossible: on restart, the outbox row is still there and still gets published.
- **Idempotency keys** — every write path (order creation, payment processing, stock reservation, saga start) is safe to retry. If a client or the orchestrator resends a request after a timeout, the original result is returned instead of double-charging or double-reserving.
- **Stuck saga recovery** — a scheduled job periodically finds sagas that stopped making progress (e.g., a saga still `STARTED` long after payment should have replied) and resends the missing step, escalating to compensation if it keeps failing.

The codebase also ships the wiring for the inverse compensation — reversing a *successful* payment (`payment.cancel.request` / `payment.cancelled`, handled by `PaymentEventListener` and `SagaOrchestrationService.processPaymentCancelled`) — even though the current step ordering never needs to trigger it, since payment is always the last forward step. It exists as the mechanism a future step added after payment (e.g., a final confirmation call to an external system) would use to unwind a charge that already succeeded. Removing this before-you-need-it compensation path from the design would have re-created the exact "payment deducted, order failed" gap the project is meant to demonstrate closing.

---

## Overview

When a customer places an order, the system must coordinate three independent services — Order, Inventory, and Payment — in a way that guarantees consistency without a distributed transaction. If any step fails, all previously completed steps must be compensated (undone).

A central **Saga Orchestrator** drives the entire flow as a persisted state machine, publishing commands to Kafka and reacting to events from each service. gRPC is used for synchronous calls that need an immediate answer (stock checks, order validation); Kafka carries the asynchronous command/event flow that makes up the saga itself.

---

## Architecture

```
Client
  |
  v REST
+----------------------+
|  Saga Orchestrator   |  (Port 8084 / gRPC 9084)
|  PostgreSQL :5435    |
+-----------+----------+
            | Kafka commands & events
   +--------+---------------+
   v        v                v
+----------+  +--------------+  +-------------+
|  Order   |  |  Inventory   |  |   Payment   |
| Service  |  |   Service    |  |   Service   |
| :8082    |  |    :8083     |  |    :8081    |
| gRPC 9082|  |  gRPC :9083  |  |  gRPC :9081 |
| PG :5433 |  |   PG :5434   |  |   PG :5436  |
+----------+  +--------------+  +-------------+
```

The orchestrator also makes direct **gRPC calls** into Order and Inventory for synchronous lookups (checking stock availability, validating order status before processing payment). All four services additionally run their own transactional-outbox poller against their own database, publishing to Kafka independently of the request path.

---

## Saga Flow: Happy Path

The successful end-to-end flow when stock is available and payment succeeds.

```
Client
  |
  |  POST /api/orders
  v
Orchestrator
  |-- gRPC --> Inventory: CheckStock         (guard: fail fast if stock is zero)
  |-- gRPC --> Inventory: GetProduct         (fetch canonical product info)
  |-- gRPC --> Order:     CreateOrder        (create order in PENDING state)
  |
  |-- Kafka --> [stock.reserve.request]
                    |
                    v
               Inventory Service
                    |  reserves stock (pessimistic lock)
                    |-- Kafka --> [stock.reserved]
                                      |
                                      v
                                 Orchestrator
                                      |-- Kafka --> [payment.process.request]
                                                        |
                                                        v
                                                   Payment Service
                                                        |  validates order via gRPC
                                                        |  processes payment
                                                        |-- Kafka --> [payment.processed]
                                                                           |
                                                                           v
                                                                      Orchestrator
                                                                           |  saga.status = COMPLETED
                                                                           |-- Kafka --> [order.confirmed]
                                                                                              |
                                                                                              v
                                                                                        Order Service
                                                                                             order.status = CONFIRMED
```

> **Final state:** `SagaInstance.status = COMPLETED`, `Order.status = CONFIRMED`

---

## Compensation Paths

There are two compensation scenarios depending on which step fails.

### Compensation Path 1 — Stock Reservation Fails

Triggered when the inventory service does not have enough stock. Nothing has been charged yet, so there is nothing to release except the order itself.

```
Orchestrator
  |-- Kafka --> [stock.reserve.request]
                    |
                    v
               Inventory Service
                    |  insufficient stock
                    |-- Kafka --> [stock.reservation.failed]
                                        |
                                        v
                                   Orchestrator
                                        |  saga.status = COMPENSATING
                                        |-- Kafka --> [order.cancelled]
                                                           |
                                                           v
                                                     Order Service
                                                          order.status = CANCELLED
```

> **Final state:** `SagaInstance.status = FAILED`

### Compensation Path 2 — Payment Fails

Triggered when the payment gateway rejects the transaction (simulated rule: total amount >= 10,000 is declined). Stock must be released before the order is cancelled — payment is charged last, so no money has actually moved and there is nothing to refund, only stock to give back.

```
Orchestrator
  |-- Kafka --> [payment.process.request]
                    |
                    v
               Payment Service
                    |  payment rejected
                    |  payment.status = FAILED
                    |-- Kafka --> [payment.failed]
                                      |
                                      v
                                 Orchestrator
                                      |  saga.status = COMPENSATING
                                      |-- Kafka --> [stock.release.request]
                                                        |
                                                        v
                                                   Inventory Service
                                                        |  releases reserved stock
                                                        |-- Kafka --> [stock.released]
                                                                           |
                                                                           v
                                                                      Orchestrator
                                                                           |-- Kafka --> [order.cancelled]
                                                                                              |
                                                                                              v
                                                                                        Order Service
                                                                                             order.status = CANCELLED
```

> **Final state:** `SagaInstance.status = FAILED`

---

## Reliability Mechanisms

These are the pieces of infrastructure that turn "coordinate three services over Kafka" into something that stays consistent when things fail mid-flight. Each one is implemented identically across all four services unless noted.

### Transactional Outbox

Every service writes its domain change (an order, a payment, a stock reservation, a saga state transition) and an `OutboxEvent` row in the **same local database transaction**, instead of writing to the database and separately publishing to Kafka as two uncoordinated steps. A scheduled poller (`outbox.poller.fixed-delay-ms=2000`, batch size `outbox.poller.batch-size=50`) claims pending rows with `FOR UPDATE SKIP LOCKED` so multiple instances of a service can run the poller concurrently without double-publishing, sends each to Kafka, and marks it published. A failed publish is retried with exponential backoff (capped at 300s) up to 5 attempts before being marked `FAILED` for manual inspection. This is what guarantees a state change and its corresponding event are never inconsistent with each other, even across a crash.

### Idempotency Keys

Order creation, payment processing, stock reservation, and saga start all accept an idempotency key and follow the same pattern: look up an existing row by that key first and return it unchanged if found; otherwise attempt to insert, and if a concurrent duplicate request wins the unique-constraint race, re-query and return that row instead of failing. The saga orchestrator derives per-step keys from the saga's own key (e.g. `<idempotencyKey>:ORDER_CREATE`, `<sagaId>:STOCK_RESERVE`, `<sagaId>:PAYMENT_PROCESS`), so a retried gRPC call or a resent Kafka command downstream is naturally deduplicated. `POST /api/orders` accepts an `Idempotency-Key` header and returns `201 Created` on the first call or `200 OK` with an "idempotent replay" message on any repeat.

### Kafka Consumer Retry and Dead-Letter Queue

Each service's Kafka listener container is configured with a `DefaultErrorHandler` backed by `DeadLetterPublishingRecoverer` and an exponential backoff of up to 3 retries (starting at 500ms, doubling each attempt). A message that still fails after retries is published to `<topic>.DLT` instead of blocking the consumer group indefinitely.

### Stuck Saga Recovery

A scheduler in the orchestrator (`saga.recovery.fixed-delay-ms=30000`, i.e. every 30 seconds) looks for sagas stuck in `STARTED` or `COMPENSATING` whose `updated_at` is older than `saga.recovery.stuck-threshold-seconds=120`, claiming one at a time with `FOR UPDATE SKIP LOCKED` (up to `saga.recovery.batch-size=20` per tick, so several orchestrator instances can run this safely in parallel). Depending on which step it is stuck on, it resends the missing command (`StockReserveCommand`, `PaymentProcessCommand`, or `StockReleaseCommand`); if a saga exceeds `saga.recovery.max-attempts=5` resends, it is escalated to `FAILED` and, where a stock reservation might still be held, compensation is triggered rather than leaving it orphaned. This is the safety net for the case a step ordering and an outbox alone can't fully cover: the orchestrator process itself stalling or restarting mid-saga.

---

## Kafka Topics

| Topic | Publisher | Consumer | Purpose |
|---|---|---|---|
| `stock.reserve.request` | Orchestrator | Inventory | Command: reserve stock |
| `stock.reserved` | Inventory | Orchestrator | Event: stock reserved |
| `stock.reservation.failed` | Inventory | Orchestrator | Event: not enough stock |
| `stock.release.request` | Orchestrator | Inventory | Compensation: release stock |
| `stock.released` | Inventory | Orchestrator | Event: stock released |
| `payment.process.request` | Orchestrator | Payment | Command: process payment |
| `payment.processed` | Payment | Orchestrator | Event: payment successful |
| `payment.failed` | Payment | Orchestrator | Event: payment rejected |
| `payment.cancel.request` | Orchestrator | Payment | Compensation: cancel an already-successful payment (wired, not yet triggered by any current step ordering) |
| `payment.cancelled` | Payment | Orchestrator | Event: payment cancelled (wired, not yet triggered by any current step ordering) |
| `order.confirmed` | Orchestrator | Order | Event: finalize as confirmed |
| `order.cancelled` | Orchestrator | Order | Compensation: cancel order |

---

## Services and Ports

| Service | REST | gRPC | DB Port | Database |
|---|---|---|---|---|
| Order Service | 8082 | 9082 | 5433 | `order_db` |
| Payment Service | 8081 | 9081 | 5436 | `payment_db` |
| Inventory Service | 8083 | 9083 | 5434 | `inventory_db` |
| Saga Orchestrator | 8084 | 9084 | 5435 | `orchestrator_db` |
| Kafka | — | — | 9092 | — |
| Kafka UI | 8090 | — | — | — |

### Key REST Endpoints

| Service | Endpoint | Purpose |
|---|---|---|
| Saga Orchestrator | `POST /api/orders` | Start a new saga (accepts optional `Idempotency-Key` header) |
| Saga Orchestrator | `GET /api/sagas/{sagaId}` | Saga status and full step history |
| Order Service | `GET /api/orders/{orderId}` | Get one order |
| Order Service | `GET /api/orders?userId={userId}` | List orders for a user |
| Payment Service | `GET /api/payments/{paymentId}` | Get one payment |
| Payment Service | `GET /api/payments/order/{orderId}` | Get payment for an order |
| Inventory Service | `POST /api/products` | Create a product with initial stock |
| Inventory Service | `GET /api/products` | List all products |
| Inventory Service | `GET /api/products/{productId}` | Get one product |

---

## Tech Stack

| Category | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3.2.3 |
| Messaging | Apache Kafka (Confluent 7.5.0), Spring Kafka |
| RPC | gRPC 1.63.0, Protocol Buffers 3.25.1 |
| Database | PostgreSQL 16 |
| ORM | Spring Data JPA / Hibernate |
| Schema Migration | Liquibase |
| Retry | Spring Retry (payment-service, saga-orchestrator) |
| Build Tool | Maven |
| Infrastructure | Docker, Docker Compose |
| Utilities | Lombok, Jackson |

There is no shared Maven reactor build — `order-service`, `payment-service`, `inventory-service`, and `saga-orchestrator` are four independent Maven projects, each depending on the `server/shared` module (gRPC/Protobuf contracts and Kafka event classes) as a regular Maven artifact. `shared` must be installed locally before the other four will build.

---

## Running the Project

**1. Build the shared module** (contracts every service depends on):

```bash
cd server/shared && mvn install
```

**2. Start infrastructure** (databases, Kafka, Zookeeper):

```bash
cd server
docker-compose up -d
```

**3. Start each service** (in separate terminals or via your IDE):

```bash
# Order Service
cd server/order-service && mvn spring-boot:run

# Inventory Service
cd server/inventory-service && mvn spring-boot:run

# Payment Service
cd server/payment-service && mvn spring-boot:run

# Saga Orchestrator
cd server/saga-orchestrator && mvn spring-boot:run
```

**4. Place an order:**

```bash
curl -X POST http://localhost:8084/api/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-001" \
  -d '{
    "userId": "user-001",
    "productId": "<product-uuid>",
    "quantity": 2,
    "unitPrice": 49.99,
    "currency": "USD"
  }'
```

**5. Query saga status:**

```bash
curl http://localhost:8084/api/sagas/{sagaId}
```

**6. Monitor Kafka topics:**

Open [http://localhost:8090](http://localhost:8090) in your browser (Kafka UI).

---

## Postman Collection

A Postman collection covering all endpoints is included at:

```
server/saga-payment-system.postman_collection.json
```

It includes dedicated scenario folders for the happy path, the insufficient-stock fast-fail path (verifying no orphan orders are created), and the payment-failure compensation path (verifying stock is restored). Import it into Postman to try every request without hand-writing curl commands.

---

## Testing

Unit tests currently cover the two areas most central to this project's premise: `OrderServiceTest` (idempotency behavior on order creation, including the concurrent-insert race) and `SagaOrchestrationServiceTest` (every branch of stuck-saga recovery, plus recovery-attempt-counter resets on forward progress). Payment-service and inventory-service currently only have context-load smoke tests beyond that — there is no integration test suite (no Testcontainers, no end-to-end Kafka/DB test) yet, which is the most useful next addition for validating the full saga end to end rather than its individual pieces.

---

## Notes

> **Amount contract:** Order requests accept `unitPrice`. The order service calculates `totalAmount = unitPrice x quantity`, and that canonical total is what the payment service charges.

> **Payment simulation:** Payments with a total amount >= 10,000 are automatically rejected, to trigger the compensation path on demand.

> **Concurrency:** The inventory service uses pessimistic locking to prevent concurrent over-reservation of the same product's stock.

> **Saga state:** Every saga step is persisted in `saga_steps` with timestamps and error messages, providing a full audit trail; `saga_instances.recovery_attempts` tracks how many times the recovery job has had to intervene.

> **Schema management:** All databases are managed via Liquibase changelogs; tables are created automatically on startup.

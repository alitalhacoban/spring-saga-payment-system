# 💳 Saga Pattern Payment System

A demo application that simulates a payment system using the **Saga Orchestration Pattern** across multiple microservices. This is **not a production-ready application** — the goal is to demonstrate how Saga pattern, microservice architecture, Apache Kafka, and gRPC can be combined in a cohesive system.

---

## 🗺️ Overview

When a customer places an order, the system must coordinate three independent services — Order, Inventory, and Payment — in a way that guarantees consistency without using a distributed transaction. If any step fails, all previously completed steps must be rolled back (compensated).

This project uses a **central Saga Orchestrator** as a state machine that drives the entire flow by publishing commands to Kafka and reacting to events from each service. gRPC is used for synchronous queries that need immediate responses; Kafka is used for the asynchronous command/event flow that makes up the saga.

---

## 🏗️ Architecture

```
Client
  │
  ▼ REST
┌─────────────────────┐
│  Saga Orchestrator  │  (Port 8084 / gRPC 9084)
│  PostgreSQL :5435   │
└────────┬────────────┘
         │ Kafka commands & events
  ┌──────┼──────────────┐
  ▼      ▼              ▼
┌──────────┐  ┌──────────────┐  ┌─────────────┐
│  Order   │  │  Inventory   │  │   Payment   │
│ Service  │  │   Service    │  │   Service   │
│ :8082    │  │    :8083     │  │    :8081    │
│ gRPC9082 │  │  gRPC :9083  │  │  gRPC :9081 │
│  PG:5433 │  │   PG :5434   │  │   PG :5436  │
└──────────┘  └──────────────┘  └─────────────┘
```

The orchestrator also makes direct **gRPC calls** to services for synchronous lookups (e.g., checking stock availability or validating an order status before processing payment).

---

## ✅ Happy Path

The successful end-to-end flow when stock is available and payment succeeds.

```
Client
  │
  │  POST /api/orders
  ▼
Orchestrator
  │── gRPC ──► Inventory: CheckStock         (guard: fail fast if stock is zero)
  │── gRPC ──► Inventory: GetProduct         (fetch canonical product info)
  │── gRPC ──► Order:     CreateOrder        (create order in PENDING state)
  │
  │── Kafka ──► [stock.reserve.request]
                    │
                    ▼
               Inventory Service
                    │  reserves stock (pessimistic lock)
                    │── Kafka ──► [stock.reserved]
                                      │
                                      ▼
                                 Orchestrator
                                      │── Kafka ──► [payment.process.request]
                                                        │
                                                        ▼
                                                   Payment Service
                                                        │  validates order via gRPC
                                                        │  processes payment
                                                        │── Kafka ──► [payment.processed]
                                                                           │
                                                                           ▼
                                                                      Orchestrator
                                                                           │  saga.status = COMPLETED
                                                                           │── Kafka ──► [order.confirmed]
                                                                                              │
                                                                                              ▼
                                                                                        Order Service
                                                                                             order.status = CONFIRMED
```

> **Final state:** `SagaInstance.status = COMPLETED` · `Order.status = CONFIRMED`

---

## ↩️ Compensation Path

There are two compensation scenarios depending on which step fails.

### Compensation Path 1 — Stock Reservation Fails

Triggered when the inventory service does not have enough stock.

```
Orchestrator
  │── Kafka ──► [stock.reserve.request]
                    │
                    ▼
               Inventory Service
                    │  insufficient stock
                    │── Kafka ──► [stock.reservation.failed]
                                        │
                                        ▼
                                   Orchestrator
                                        │  saga.status = COMPENSATING
                                        │── Kafka ──► [order.cancelled]
                                                           │
                                                           ▼
                                                     Order Service
                                                          order.status = CANCELLED
```

> **Final state:** `SagaInstance.status = FAILED`

---

### Compensation Path 2 — Payment Fails

Triggered when the payment gateway rejects the transaction (simulated: amounts >= 10,000 fail). Stock must be released before cancelling the order.

```
Orchestrator
  │── Kafka ──► [payment.process.request]
                    │
                    ▼
               Payment Service
                    │  payment rejected
                    │  payment.status = FAILED
                    │── Kafka ──► [payment.failed]
                                      │
                                      ▼
                                 Orchestrator
                                      │  saga.status = COMPENSATING
                                      │── Kafka ──► [stock.release.request]
                                                        │
                                                        ▼
                                                   Inventory Service
                                                        │  releases reserved stock
                                                        │── Kafka ──► [stock.released]
                                                                           │
                                                                           ▼
                                                                      Orchestrator
                                                                           │── Kafka ──► [order.cancelled]
                                                                                              │
                                                                                              ▼
                                                                                        Order Service
                                                                                             order.status = CANCELLED
```

> **Final state:** `SagaInstance.status = FAILED`

---

## 📨 Kafka Topics

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
| `payment.cancel.request` | Orchestrator | Payment | Compensation: cancel payment |
| `payment.cancelled` | Payment | Orchestrator | Event: payment cancelled |
| `order.confirmed` | Orchestrator | Order | Event: finalize as confirmed |
| `order.cancelled` | Orchestrator | Order | Compensation: cancel order |

---

## 🔌 Services & Ports

| Service | REST | gRPC | DB Port | Database |
|---|---|---|---|---|
| 📦 Order Service | 8082 | 9082 | 5433 | `order_db` |
| 💰 Payment Service | 8081 | 9081 | 5436 | `payment_db` |
| 🏪 Inventory Service | 8083 | 9083 | 5434 | `inventory_db` |
| 🎯 Saga Orchestrator | 8084 | 9084 | 5435 | `orchestrator_db` |
| 🟠 Kafka | — | — | 9092 | — |
| 🖥️ Kafka UI | 8090 | — | — | — |

---

## 🛠️ Tech Stack

| Category | Technology |
|---|---|
| ☕ Language | Java 17 |
| 🍃 Framework | Spring Boot 3.2.3 |
| 📨 Messaging | Apache Kafka (Confluent 7.5.0), Spring Kafka |
| 🔗 RPC | gRPC 1.63.0, Protocol Buffers 3.25.1 |
| 🐘 Database | PostgreSQL 16 |
| 🗄️ ORM | Spring Data JPA / Hibernate |
| 🔄 Schema Migration | Liquibase |
| 🔨 Build Tool | Maven |
| 🐳 Infrastructure | Docker, Docker Compose |
| 🧰 Utilities | Lombok, Jackson |

---

## 🚀 Running the Project

**1. Start infrastructure** (databases, Kafka, Zookeeper):

```bash
cd server
docker-compose up -d
```

**2. Start each service** (in separate terminals or via your IDE):

```bash
# 📦 Order Service
cd server/order-service && mvn spring-boot:run

# 🏪 Inventory Service
cd server/inventory-service && mvn spring-boot:run

# 💰 Payment Service
cd server/payment-service && mvn spring-boot:run

# 🎯 Saga Orchestrator
cd server/saga-orchestrator && mvn spring-boot:run
```

**3. Place an order:**

```bash
curl -X POST http://localhost:8084/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "user-001",
    "productId": "<product-uuid>",
    "quantity": 2
  }'
```

**4. Query saga status:**

```bash
curl http://localhost:8084/api/sagas/{sagaId}
```

**5. Monitor Kafka topics:**

Open [http://localhost:8090](http://localhost:8090) in your browser (Kafka UI).

---

## 🧪 Postman Collection

A Postman collection covering all endpoints is included at:

```
server/saga-payment-system.postman_collection.json
```

Import it into Postman to quickly try out every request — creating products, placing orders (happy path & compensation), querying saga/order/payment status, and more. No manual curl commands needed.

---

## 📝 Notes

> **Payment simulation:** Payments with a total amount >= 10,000 are automatically rejected to trigger the compensation path.

> **Concurrency:** The inventory service uses pessimistic locking to prevent concurrent over-reservation.

> **Saga state:** Every saga step is persisted in `saga_steps` with timestamps and error messages, providing a full audit trail.

> **Schema management:** All databases are managed via Liquibase changelogs; tables are created automatically on startup.

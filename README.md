# Transaction Processing System
### Spring Boot 3 · Apache Kafka · PostgreSQL · JWT · Resilience4j

A production-grade, event-driven financial transaction processing system built on a microservices-inspired architecture. Extends concepts from the JPMorgan Chase Forage virtual experience with full security, resilience, observability, and containerisation.

---

## Table of Contents
1. [Architecture Overview](#1-architecture-overview)
2. [Component Deep-Dive](#2-component-deep-dive)
3. [Security Design](#3-security-design)
4. [Resilience Patterns](#4-resilience-patterns)
5. [Fraud Detection Engine](#5-fraud-detection-engine)
6. [Kafka Event Pipeline](#6-kafka-event-pipeline)
7. [API Reference](#7-api-reference)
8. [Data Model](#8-data-model)
9. [Local Setup](#9-local-setup)
10. [Environment Variables](#10-environment-variables)
11. [Shipping to Another Environment](#11-shipping-to-another-environment)
12. [Observability](#12-observability)
13. [Testing Strategy](#13-testing-strategy)
14. [Design Decisions](#14-design-decisions)

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                          CLIENT (HTTP)                                │
│                    POST /api/transactions                             │
└─────────────────────────────┬────────────────────────────────────────┘
                              │  Bearer JWT
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                   SPRING SECURITY FILTER CHAIN                        │
│  JwtAuthenticationFilter → validates token → sets SecurityContext     │
└─────────────────────────────┬────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     TRANSACTION CONTROLLER                            │
│         @Valid validation → route to TransactionService               │
└──────────┬──────────────────────────┬────────────────────────────────┘
           │                          │
           ▼                          ▼
┌──────────────────────┐   ┌──────────────────────────────────────────┐
│  FRAUD DETECTION     │   │          TRANSACTION SERVICE              │
│  SERVICE             │   │  1. Pre-screen fraud                      │
│                      │   │  2. Persist to PostgreSQL                 │
│  R1: Amount limit    │   │  3. Publish Kafka event                   │
│  R2: Velocity check  │   └─────────────────┬────────────────────────┘
│  R3: Daily volume    │                     │
│  R4: Repeat amount   │                     ▼
│  (O(1) hashmap)      │   ┌──────────────────────────────────────────┐
└──────────────────────┘   │            KAFKA PRODUCER                 │
                           │  @Retry(Resilience4j, 3 attempts)        │
                           │  Key = accountId (ordered per account)   │
                           └─────────────────┬────────────────────────┘
                                             │
                           ┌─────────────────▼────────────────────────┐
                           │         KAFKA TOPIC: transactions         │
                           │         6 partitions, key=accountId      │
                           └──────────┬──────────────────┬────────────┘
                                      │  success         │  failure (after retries)
                                      ▼                  ▼
                    ┌─────────────────────────┐  ┌──────────────────────────────┐
                    │  TRANSACTION CONSUMER   │  │  DEAD LETTER TOPIC (.DLT)    │
                    │  Manual ack             │  │  DeadLetterQueueHandler      │
                    │  Re-evaluate fraud      │  │  Logs + future: incident DB  │
                    │  Update DB status       │  └──────────────────────────────┘
                    └────────────┬────────────┘
                                 │
                                 ▼
                    ┌─────────────────────────┐
                    │      POSTGRESQL         │
                    │  transactions table     │
                    │  Indexes: status,       │
                    │  accountId, createdAt   │
                    └─────────────────────────┘
```

### Request Lifecycle (numbered)

```
 1  Client sends POST /api/transactions with Bearer JWT
 2  JwtAuthenticationFilter validates token signature + expiry
 3  Bean Validation (@Valid) checks all DTO fields
 4  FraudDetectionService runs 4 rules; throws if flagged
 5  TransactionService saves PENDING record to PostgreSQL
 6  TransactionProducer sends event to Kafka (key = accountId)
    └─ Resilience4j retries up to 3× on broker failure
 7  TransactionConsumer receives event (manual ack)
    └─ Re-runs fraud rules
    └─ Updates status → COMPLETED or FLAGGED
    └─ Commits offset only on success
 8  On repeated consumer failure → DeadLetterPublishingRecoverer
    routes message to transactions.DLT topic
 9  DeadLetterQueueHandler logs and stores for manual review
```

---

## 2. Component Deep-Dive

### Package Layout

```
src/main/java/com/example/transaction/
│
├── TransactionApplication.java       Entry point + @EnableRetry
│
├── config/
│   ├── KafkaConfig.java              Producer, consumer, DLT, topic beans
│   ├── SecurityConfig.java           Filter chain, BCrypt, method security
│   └── OpenApiConfig.java            Swagger/OpenAPI setup
│
├── controller/
│   └── TransactionController.java    REST endpoints (POST, GET, GET list)
│
├── dto/
│   ├── TransactionRequest.java       Inbound; fully annotated with @Valid rules
│   ├── TransactionResponse.java      Outbound; no direct entity exposure
│   ├── AuthRequest.java              Login credentials
│   └── AuthResponse.java            Token + metadata
│
├── entity/
│   └── Transaction.java              JPA entity; enums for status/type
│
├── exception/
│   ├── FraudDetectedException.java   422 Unprocessable Entity
│   ├── TransactionNotFoundException  404 Not Found
│   └── GlobalExceptionHandler.java   RFC 7807 Problem Details for all errors
│
├── fraud/
│   └── FraudDetectionService.java    4-rule engine with ConcurrentHashMap cache
│
├── kafka/
│   ├── TransactionEvent.java         Serializable Kafka message (not the entity)
│   ├── TransactionProducer.java      Publishes events with @Retry fallback
│   ├── TransactionConsumer.java      Processes events; manual offset ack
│   └── DeadLetterQueueHandler.java   Consumes .DLT topic
│
├── repository/
│   └── TransactionRepository.java    JPA repository; custom JPQL queries
│
├── security/
│   ├── JwtTokenProvider.java         Token generation + validation (jjwt 0.12)
│   ├── JwtAuthenticationFilter.java  OncePerRequestFilter; populates SecurityContext
│   └── AuthController.java           POST /api/auth/login
│
├── service/
│   └── TransactionService.java       Core orchestration logic
│
└── validator/
    ├── ValidAmount.java              Custom annotation
    ├── AmountValidator.java          Scale + positivity check
    ├── ValidCurrency.java            Custom annotation
    └── CurrencyValidator.java        ISO 4217 check via JDK Currency class
```

---

## 3. Security Design

### JWT Authentication Flow

```
Client                    AuthController              JwtTokenProvider
  │                            │                            │
  │── POST /api/auth/login ──► │                            │
  │   { username, password }   │── authenticate() ─────────►│
  │                            │◄─ UserDetails ─────────────│
  │                            │── generateToken(ud) ───────►│
  │                            │◄─ signed JWT ──────────────│
  │◄── { token, expiresIn } ───│                            │
  │                            │                            │
  │── POST /api/transactions ──────────────────────────────►│
  │   Authorization: Bearer <token>                          │
  │                       JwtAuthenticationFilter            │
  │                            │── validateToken() ─────────►│
  │                            │◄─ true/false ──────────────│
  │                            │── sets SecurityContext       │
```

### Security Layers Applied

| Layer | Mechanism | Implementation |
|---|---|---|
| Authentication | JWT HMAC-SHA256 | `JwtTokenProvider` + `JwtAuthenticationFilter` |
| Password storage | BCrypt (cost 12) | `SecurityConfig.passwordEncoder()` |
| Input validation | Bean Validation + custom validators | `TransactionRequest` + `AmountValidator`, `CurrencyValidator` |
| SQL injection | Spring Data JPA prepared statements | All `TransactionRepository` queries |
| Session fixation | Stateless — no session cookies | `SessionCreationPolicy.STATELESS` |
| Method-level authz | `@PreAuthorize("hasRole(...)")` | `@EnableMethodSecurity` |
| Security headers | HSTS, X-Content-Type-Options, Referrer-Policy | `SecurityConfig.filterChain()` |
| Secret management | Environment variables only | `${JWT_SECRET}` — no fallback default |
| Error exposure | RFC 7807 problem details; no stack traces | `GlobalExceptionHandler` |

### Why JWT Secret Must Be an Environment Variable

```
# WRONG — never do this
jwt.secret=myHardcodedSecret123

# CORRECT — inject from environment
jwt.secret=${JWT_SECRET}    # fails fast on startup if not set

# Generate a cryptographically strong secret:
openssl rand -base64 64
```

The `JwtTokenProvider` constructor validates the key is at least 256 bits and throws `IllegalStateException` at startup if it is not — preventing the app from running with a weak secret.

---

## 4. Resilience Patterns

### Resilience4j Retry — Producer

```
Kafka broker unavailable?
        │
        ├── Attempt 1 ──── wait 1s ────►
        │                               fail
        ├── Attempt 2 ──── wait 2s ────►
        │                               fail
        ├── Attempt 3 ──── wait 4s ────►
        │                               fail
        └── producerFallback() called
            └── logs + returns failed CompletableFuture
```

Configuration (application.yml):
```yaml
resilience4j:
  retry:
    instances:
      kafkaProducer:
        max-attempts: 3
        wait-duration: 1s
        exponential-backoff-multiplier: 2.0
        retry-exceptions:
          - org.apache.kafka.common.errors.TimeoutException
          - org.springframework.kafka.KafkaException
```

### Consumer Error Handler + Dead Letter Topic

```
Consumer receives message
        │
        ├── Processing attempt 1 ──── fail ──── wait 1s ───►
        ├── Processing attempt 2 ──── fail ──── wait 2s ───►
        ├── Processing attempt 3 ──── fail ──── wait 4s ───►
        │                                                    │
        └─── DeadLetterPublishingRecoverer ─────────────────►
                        │
                        ▼
             topic: transactions.DLT
                        │
                        ▼
             DeadLetterQueueHandler.handleDeadLetter()
             └── logs error with context
             └── TODO: persist to failed_events table
```

### Kafka Producer Guarantees

| Config | Value | Why |
|---|---|---|
| `enable.idempotence` | `true` | Prevents duplicate writes to Kafka |
| `acks` | `all` | Waits for all in-sync replicas to confirm |
| `retries` | `3` | Network hiccups handled at transport level |
| `max.in.flight.requests.per.connection` | `1` | Preserves ordering with retries enabled |

---

## 5. Fraud Detection Engine

### Rules & Complexity

```
Transaction arrives at FraudDetectionService.evaluate()
        │
        ├─ R1: amount >= threshold?          O(1) — simple comparison
        │       └── YES → FLAGGED("Amount exceeds threshold")
        │
        ├─ R2: recent tx count >= velocity limit?  O(log n) — indexed DB query
        │       └── YES → FLAGGED("Velocity limit exceeded")
        │
        ├─ R3: daily cumulative amount > cap?  O(log n) — indexed DB query
        │       └── YES → FLAGGED("Daily cap exceeded")
        │
        ├─ R4: same amount as last tx?        O(1) — ConcurrentHashMap.get()
        │       └── YES → FLAGGED("Duplicate amount")
        │
        └─ CLEAN → proceed
```

### Why ConcurrentHashMap for R4

```java
// Thread-safe O(1) average-case lookup — no lock on read under JDK 8+
private final ConcurrentHashMap<String, BigDecimal> lastAmountCache = new ConcurrentHashMap<>();

BigDecimal lastAmount = lastAmountCache.get(accountId);   // O(1)
lastAmountCache.put(accountId, amount);                   // O(1)
```

`ConcurrentHashMap` uses segment/bucket locking — reads are effectively lock-free and writes lock only the affected bucket. Under high concurrency this vastly outperforms a `synchronized HashMap`.

### Configurable Thresholds (via environment)

| Variable | Default | Meaning |
|---|---|---|
| `FRAUD_AMOUNT_THRESHOLD` | 10,000 | Single-tx hard limit |
| `FRAUD_VELOCITY_WINDOW_MINUTES` | 10 | Rolling window for count check |
| `FRAUD_VELOCITY_MAX_COUNT` | 10 | Max transactions in window |
| `FRAUD_DAILY_VOLUME_CAP` | 50,000 | Max cumulative daily amount |

---

## 6. Kafka Event Pipeline

### Topic Design

```
Topic: transactions
├── Partitions: 6
├── Key: accountId
│   └── All events for an account land on the same partition → ordered delivery
├── Replication factor: 1 (increase to 3 in production)
└── Retention: 7 days

Topic: transactions.DLT
├── Partitions: 1
└── Consumed by DeadLetterQueueHandler
```

### Message Schema (TransactionEvent)

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "accountId": "ACC-001",
  "amount": 250.00,
  "currency": "USD",
  "type": "DEBIT",
  "status": "PENDING",
  "merchantId": "MERCH-42",
  "referenceId": "REF-2024-001",
  "createdAt": "2024-01-15T10:30:00Z",
  "flaggedForReview": false,
  "fraudReason": null
}
```

**Important**: The Kafka message is a `TransactionEvent` DTO — never the JPA `Transaction` entity. This decouples the messaging contract from the persistence schema.

### Offset Commit Strategy

```
Manual acknowledgment (AckMode.MANUAL_IMMEDIATE)

Consumer receives message
        │
        ├── processEvent() — fraud check, DB update
        │       │
        │       ├── SUCCESS → ack.acknowledge() → offset committed
        │       │
        │       └── FAILURE → exception thrown → NO ack → retry
```

This guarantees at-least-once delivery. Idempotency (deduplication) is handled by checking transaction status before reprocessing.

---

## 7. API Reference

### Authentication

#### POST /api/auth/login
```bash
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"changeme"}'
```
Response:
```json
{
  "token": "eyJhbGciOiJIUzI1NiJ9...",
  "tokenType": "Bearer",
  "expiresIn": 3600
}
```

### Transactions

All transaction endpoints require: `Authorization: Bearer <token>`

#### POST /api/transactions — Create transaction
```bash
curl -X POST http://localhost:8080/api/transactions \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "ACC-001",
    "amount": 250.00,
    "currency": "USD",
    "type": "DEBIT",
    "description": "Monthly subscription",
    "merchantId": "MERCH-42"
  }'
```
Response `201 Created`:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "accountId": "ACC-001",
  "amount": 250.00,
  "currency": "USD",
  "type": "DEBIT",
  "status": "PENDING",
  "flaggedForReview": false,
  "createdAt": "2024-01-15T10:30:00Z",
  "updatedAt": "2024-01-15T10:30:00Z"
}
```

#### GET /api/transactions/{id}
```bash
curl http://localhost:8080/api/transactions/550e8400-e29b-41d4-a716-446655440000 \
  -H "Authorization: Bearer <token>"
```

#### GET /api/transactions — List with filters
```bash
# By account
curl "http://localhost:8080/api/transactions?accountId=ACC-001&page=0&size=20&sort=createdAt,desc" \
  -H "Authorization: Bearer <token>"

# By status
curl "http://localhost:8080/api/transactions?status=FLAGGED" \
  -H "Authorization: Bearer <token>"
```

### Validation Error Response (RFC 7807)
```json
{
  "type": "https://problems.example.com/validation-error",
  "title": "Validation Failed",
  "status": 400,
  "detail": "One or more fields failed validation",
  "errors": {
    "amount": "Amount must be greater than 0",
    "currency": "Currency must be a valid ISO 4217 3-letter code"
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

### Fraud Rejection Response
```json
{
  "type": "https://problems.example.com/fraud-detected",
  "title": "Transaction Rejected",
  "status": 422,
  "detail": "Transaction rejected by fraud engine: Amount exceeds single-transaction threshold of 10000",
  "timestamp": "2024-01-15T10:30:00Z"
}
```

---

## 8. Data Model

### transactions table

```sql
CREATE TABLE transactions (
    id              UUID PRIMARY KEY,
    account_id      VARCHAR NOT NULL,
    amount          NUMERIC(19,4) NOT NULL,
    currency        VARCHAR(3) NOT NULL,
    type            VARCHAR NOT NULL,        -- CREDIT | DEBIT | TRANSFER | REFUND
    status          VARCHAR NOT NULL,        -- PENDING | PROCESSING | COMPLETED | FAILED | FLAGGED
    description     VARCHAR(500),
    merchant_id     VARCHAR(64),
    reference_id    VARCHAR(128),
    flagged_for_review BOOLEAN NOT NULL DEFAULT FALSE,
    fraud_reason    VARCHAR,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    version         BIGINT                   -- optimistic locking
);

CREATE INDEX idx_transactions_status     ON transactions(status);
CREATE INDEX idx_transactions_account_id ON transactions(account_id);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);
```

### Status State Machine

```
                    ┌─────────────────────────────────────────┐
                    │              PENDING                      │
                    │  (created by service, event published)   │
                    └──────────────────┬──────────────────────┘
                                       │ consumer picks up
                                       ▼
                    ┌─────────────────────────────────────────┐
                    │            PROCESSING                    │
                    └──────────┬─────────────────┬────────────┘
                               │ clean           │ fraud hit
                               ▼                 ▼
                    ┌────────────────┐  ┌────────────────────┐
                    │   COMPLETED    │  │      FLAGGED        │
                    └────────────────┘  │ (manual review)    │
                                        └────────────────────┘
```

---

## 9. Local Setup

### Prerequisites

| Tool | Minimum Version |
|---|---|
| JDK | 17 |
| Maven | 3.8 |
| Docker | 24 |
| Docker Compose | v2 |

### 1. Clone & configure

```bash
git clone <repository-url>
cd java-transaction-system
```

Create a `.env` file (never commit this):
```bash
# .env
JWT_SECRET=$(openssl rand -base64 64)
DB_USERNAME=user
DB_PASSWORD=password
FRAUD_AMOUNT_THRESHOLD=10000
```

### 2. Start infrastructure + app

```bash
# First build: pulls images, compiles, starts everything
docker compose --env-file .env up --build

# Background mode
docker compose --env-file .env up -d --build
```

Services started:
| Service | Port |
|---|---|
| Application | 8080 |
| PostgreSQL | 5432 |
| Kafka | 9092 |
| Zookeeper | 2181 |

### 3. Verify

```bash
# Health
curl http://localhost:8080/actuator/health

# Swagger UI
open http://localhost:8080/swagger-ui.html

# Get a token
curl -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"changeme"}'

# Create a transaction
curl -X POST http://localhost:8080/api/transactions \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"accountId":"ACC-001","amount":100.00,"currency":"USD","type":"DEBIT"}'
```

### 4. Local dev (without Docker for the app)

```bash
# Start only infrastructure
docker compose up postgres kafka zookeeper -d

# Run app with local profile
export JWT_SECRET=$(openssl rand -base64 64)
export DATABASE_URL=jdbc:postgresql://localhost:5432/transactions
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

---

## 10. Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `JWT_SECRET` | **YES** | none | Base64 key ≥256 bits. Generate: `openssl rand -base64 64` |
| `DATABASE_URL` | **YES** | — | Full JDBC URL, e.g. `jdbc:postgresql://host:5432/db` |
| `DB_USERNAME` | YES | `user` | Database username |
| `DB_PASSWORD` | YES | — | Database password |
| `KAFKA_BOOTSTRAP_SERVERS` | YES | `localhost:9092` | Kafka broker address(es) |
| `KAFKA_TOPIC_TRANSACTIONS` | no | `transactions` | Main topic name |
| `JWT_EXPIRATION_MS` | no | `3600000` | Token lifetime in ms (default 1h) |
| `JWT_ISSUER` | no | `transaction-system` | JWT `iss` claim value |
| `JPA_DDL_AUTO` | no | `validate` | Hibernate DDL mode; use `create-drop` locally |
| `FRAUD_AMOUNT_THRESHOLD` | no | `10000` | Single-tx amount limit |
| `FRAUD_VELOCITY_WINDOW_MINUTES` | no | `10` | Rolling window for velocity check |
| `FRAUD_VELOCITY_MAX_COUNT` | no | `10` | Max transactions in window |
| `FRAUD_DAILY_VOLUME_CAP` | no | `50000` | Daily cumulative cap |
| `LOG_LEVEL` | no | `INFO` | Application log level |
| `SPRING_PROFILES_ACTIVE` | no | `default` | Active Spring profile |

---

## 11. Shipping to Another Environment

### Cloud (Railway, Render, Fly.io, AWS ECS)

The app is a standard Docker image — deploy anywhere Docker runs.

```bash
# Build the image
docker build -t transaction-system:1.0 .

# Tag for your registry
docker tag transaction-system:1.0 <your-registry>/transaction-system:1.0

# Push
docker push <your-registry>/transaction-system:1.0
```

Set all required environment variables in your platform's secrets manager. The app fails fast on startup if `JWT_SECRET` or `DATABASE_URL` are missing.

### Kubernetes

```yaml
# deployment.yaml (minimal example)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: transaction-system
spec:
  replicas: 2
  selector:
    matchLabels:
      app: transaction-system
  template:
    metadata:
      labels:
        app: transaction-system
    spec:
      containers:
        - name: app
          image: <registry>/transaction-system:1.0
          ports:
            - containerPort: 8080
          env:
            - name: JWT_SECRET
              valueFrom:
                secretKeyRef:
                  name: transaction-secrets
                  key: jwt-secret
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: transaction-secrets
                  key: database-url
          livenessProbe:
            httpGet:
              path: /actuator/health
              port: 8080
            initialDelaySeconds: 60
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /actuator/health
              port: 8080
            initialDelaySeconds: 30
```

### CI/CD Checklist

- [ ] `JWT_SECRET` stored in CI secrets (GitHub Actions: Settings → Secrets)
- [ ] `DATABASE_URL` pointing to managed PostgreSQL (e.g. AWS RDS, Supabase)
- [ ] `KAFKA_BOOTSTRAP_SERVERS` pointing to managed Kafka (e.g. Confluent Cloud, MSK)
- [ ] `JPA_DDL_AUTO=validate` in production (run Flyway/Liquibase for schema migrations)
- [ ] Kafka replication factor set to 3 in production
- [ ] Image vulnerability scanning (e.g. Trivy) in pipeline

---

## 12. Observability

### Health Check
```bash
GET /actuator/health
```
Returns component-level status (DB, Kafka) when authenticated.

### Prometheus Metrics
```bash
GET /actuator/prometheus
```
Exposes JVM, HTTP, Kafka consumer lag, and custom counters.

### Key Metrics to Monitor

| Metric | Alert Threshold |
|---|---|
| `http_server_requests_seconds_max` | > 2s |
| `kafka_consumer_lag` | > 1000 |
| `jvm_memory_used_bytes` | > 80% of limit |
| `hikaricp_connections_active` | > 80% of pool |
| `process_cpu_usage` | > 80% |

### Structured Log Fields

All log lines include:
- ISO 8601 timestamp
- Thread name
- Level
- Logger class
- Message

Sensitive data (passwords, full card/account numbers) is never logged.

---

## 13. Testing Strategy

```
src/test/java/com/example/transaction/
├── TransactionServiceTest.java    Unit tests — mocked dependencies
└── TransactionControllerTest.java  Web layer tests — @WebMvcTest + MockMvc
```

### Run Tests
```bash
./mvnw test

# With coverage report
./mvnw test jacoco:report
```

### Test Coverage Areas

| Layer | Test Type | Tools |
|---|---|---|
| Service | Unit | JUnit 5 + Mockito |
| Controller | Slice | @WebMvcTest + MockMvc + Spring Security Test |
| Validation | Unit (via controller slice) | MockMvc + invalid payloads |
| Fraud rules | Unit | JUnit 5 + Mockito |
| Integration (optional) | Full stack | Testcontainers (Kafka + PostgreSQL) |

---

## 14. Design Decisions

### Why Kafka instead of direct DB write?

Decouples the write path from the processing path. The HTTP response returns immediately after DB + event publish. Processing (fraud re-check, status update) happens asynchronously. This allows the API to remain responsive even if processing is slow or retrying.

### Why manual Kafka offset acknowledgment?

Auto-commit could mark an offset as processed before the DB write completes, losing the event on crash. Manual ack (`AckMode.MANUAL_IMMEDIATE`) commits only after both fraud check and DB update succeed.

### Why TransactionEvent instead of serializing the entity?

The JPA entity has Hibernate proxy internals and bidirectional relationships that Jackson cannot serialize safely. The event DTO is a plain, immutable snapshot — predictable, versionable, and independent of the DB schema.

### Why ConcurrentHashMap for R4 fraud rule?

Database round-trips for every request under high load would bottleneck fraud detection. The repeat-amount cache gives O(1) lookups with no lock contention on reads (ConcurrentHashMap's segment design). It trades perfect accuracy (in-process, not distributed) for throughput. For a multi-node deployment, replace with Redis.

### Why RFC 7807 Problem Details?

Consistent, machine-readable error format. Clients can branch on `type` URI without parsing free-form error strings. Error responses never include stack traces.

### Why BCrypt cost 12?

OWASP recommendation for 2024 hardware. Cost 12 takes ~250ms — sufficient to slow brute-force attacks without noticeably affecting login UX (login is infrequent).

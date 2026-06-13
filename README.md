#### Architecture Documentation

For detailed architectural decisions, trade-off analysis, sequence diagrams, messaging patterns, and workflow design, see:
- https://github.com/sandesh-engineering/realtime-food-delivery-system-design 

# Orchestrator Service

A workflow orchestration service responsible for coordinating distributed business transactions across multiple microservices using the Saga Orchestration Pattern.

The service acts as the central coordinator for business workflows by issuing commands, consuming events, managing workflow state, and handling compensation logic in a controlled and observable manner.

---

## Overview

The Orchestrator Service was built to solve a common challenge in microservice architectures: coordinating business workflows that span multiple services while maintaining service autonomy.

Rather than allowing services to communicate directly with one another, the orchestrator manages workflow execution through explicit commands, events, and state transitions.

Current workflows include:

### Dispatch Workflow

1. Payment completed
2. Restaurant confirmation
3. Dispatch creation
4. Agent notification
5. Agent assignment confirmation
6. Workflow completion

---

## Architecture

```text
Payment Service
       │
       │ PaymentCompleted
       ▼
┌────────────────────┐
│ Orchestrator       │
└────────────────────┘
       │
       ▼
     RabbitMQ
       │
 ┌─────┴─────┐
 ▼           ▼
Order     Dispatch
Service   Service

       │
       ▼
  PostgreSQL
```

---

## Core Concepts

### Saga Orchestration

The orchestrator coordinates distributed transactions using the Saga Orchestration Pattern.

Benefits:

* Centralized workflow visibility
* Explicit state transitions
* Simplified failure handling
* Compensation support
* Reduced service coupling

### Transactional Outbox

Workflow state changes and event publication are coordinated using the Transactional Outbox Pattern to ensure reliable message delivery.

### Event-Driven Communication

RabbitMQ is used as the transport layer for commands and events exchanged between services.

### Reliability

The service includes:

* Idempotent event processing
* Retry mechanisms
* Dead-letter queue handling
* Persistent saga state tracking
* Publisher backpressure control

---

## Dispatch Workflow

```text
PAYMENT_COMPLETED
         │
         ▼
AWAITING_RESTAURANT_CONFIRMATION
         │
         ▼
CREATE_DISPATCH
         │
         ▼
NOTIFY_AGENTS
         │
         ▼
CONFIRM_AGENT_ASSIGNMENT
         │
         ▼
COMPLETED
```

---

## Technology Stack

* Node.js
* TypeScript
* PostgreSQL
* RabbitMQ
* Docker
* GitHub Actions
* AWS (ECR + EC2) (Planned) 

---

## Current Status

Implemented:

* Saga Orchestration
* Transactional Outbox Pattern
* RabbitMQ Integration
* Retry Handling
* Dead Letter Queue Support
* Idempotent Processing
* State-Driven Workflow Execution

In Progress:

* Distributed Tracing
* Subscription Saga
* Deployment Automation

Planned:

* RabbitMQ ACLs
* OpenTelemetry
* Workflow Versioning
* ECS Deployment

---

## Lessons Learned

This project evolved through multiple design iterations and helped explore:

* Saga Orchestration
* Distributed Transactions
* Transactional Outbox Pattern
* Event-Driven Architecture
* RabbitMQ Routing Strategies
* Reliable Event Publishing
* Workflow State Management
* Failure Recovery Patterns

```

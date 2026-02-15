# conduit API Contract (Phase 0)

This document freezes the initial API behavior and ownership rules for the scaffolding milestone.

## Threading Model

- Current implementation is single-threaded at bus dispatch.
- `cd_bus_pump` must be called from one thread at a time.
- Concurrent publish/subscribe is not yet guaranteed thread-safe in Phase 0.

## Memory Ownership

- Callers own input payload memory passed into `cd_publish` and `cd_send_command`.
- The bus copies payload bytes into queue-owned memory during enqueue.
- Subscriber callbacks receive read-only payload pointers (`cd_envelope_t.payload`).
- Message payload memory is released by the bus after dispatch completes.

## Delivery Semantics (Phase 0)

- `cd_publish` enqueues an event; delivery happens when `cd_bus_pump` is called.
- Events are delivered to all subscribers that match topic and kind mask.
- Commands are delivered to subscribers matching topic, kind mask, and target endpoint.
- Subscription callback invocation order is deterministic by subscription registration id.
- `cd_bus_pump` drains a stable queue snapshot taken at call start.
- Messages published during callback execution are deferred to a later pump call.

## Status and Error Behavior

- `CD_STATUS_INVALID_ARGUMENT`: null pointers or invalid required fields.
- `CD_STATUS_ALLOCATION_FAILED`: memory allocation failure.
- `CD_STATUS_QUEUE_FULL`: enqueue attempt beyond queue capacity.
- `CD_STATUS_CAPACITY_REACHED`: subscription table full.
- `CD_STATUS_NOT_FOUND`: unsubscribe unknown subscription id.
- `CD_STATUS_NOT_IMPLEMENTED`: request/reply APIs in this phase.

## Phase Boundaries

- Request/reply (`cd_request_async`, `cd_poll_reply`) is reserved for Phase 2.
- Transport adapters are reserved for later milestones.

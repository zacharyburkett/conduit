# Conduit Architecture

Use this document to understand how Conduit is structured internally and why.

## Repository Layout (Target)

- `include/conduit/`: public API
- `src/`: core implementation
- `src/transport/`: transport adapters
- `apps/broker/`: optional standalone bridge/broker executable
- `tests/`: unit/integration tests
- `docs/`: architecture and plan docs

## High-Level Model

`conduit` is a message fabric with a stable envelope contract and pluggable transport:

1. Local bus handles routing and dispatch policy.
2. Transport adapters connect one bus to other buses/processes.
3. Optional broker process routes across standalone tools.

Modules should only depend on the core API, not specific transports.

## Message Envelope Contract

Every message carries:

- `message_id`: unique per process/bus instance
- `correlation_id`: ties request to reply
- `kind`: event, command, request, reply
- `topic`: numeric/topic-id route key
- `source_endpoint`
- `target_endpoint` (optional for directed messages)
- `schema_id` + `schema_version`
- `flags` (priority, reliability, local-only, etc.)
- `timestamp_ns` (monotonic clock)
- `payload_size`
- `payload` bytes

Why this shape:
- Works for in-process and cross-process.
- Supports gameplay and tool traffic with one contract.
- Keeps schema evolution explicit.

## Core Runtime Objects

- `cd_context_t`: global runtime + allocator + clocks
- `cd_bus_t`: local router + queues + subscriber registry
- `cd_endpoint_t`: module identity + receive policy
- `cd_subscription_t`: topic -> callback binding
- `cd_transport_t`: pluggable send/poll/flush operations

## Dispatch Semantics

Two dispatch paths:

1. Deferred dispatch (default for gameplay):
  - Publish enqueues.
  - `cd_bus_pump()` drains in deterministic order.
  - Called in frame/update loop.

2. Immediate dispatch (tool/control paths):
  - Synchronous callback invocation.
  - Restricted to non-recursive routing rules to avoid deep reentry.

Ordering guarantees (MVP):
- FIFO per topic queue.
- Stable subscriber order by registration sequence.
- Deterministic tie-breaker by endpoint id.

## Routing Model

Message classes:

- Event:
  - Published to topic.
  - Delivered to all subscribers.
- Command:
  - Directed to one endpoint (optional topic for tracing).
- Request:
  - Directed to one endpoint with timeout.
  - Requires a reply with matching `correlation_id`.
- Reply:
  - Routed to original requester mailbox.

MVP excludes wildcard topic matching and content-based filters.

## Transport Abstraction

Transport interface supports:

- `send(envelope)`
- `poll(incoming_batch)`
- `flush()`
- `shutdown()`

Initial adapters:

- `inproc` (no serialization boundary; copy envelope into local queues)
- `ipc_socket` (Unix domain sockets or named pipes)

Future adapters:

- TCP/UDP for remote tooling if needed.

## Serialization Strategy

Payload is transport-neutral bytes with schema metadata.

Recommended schema strategy:
- Manually versioned C structs for in-process fast path.
- Optional packed binary format for IPC (explicit endian and alignment rules).

Never send raw process pointers across transport boundaries.

## Memory and Ownership

- API accepts allocator callbacks at context creation.
- Publishing copies payload into queue-owned memory.
- Consumer callback receives read-only payload view.
- Acknowledge/return from callback releases message memory.

Optional later optimization:
- Arena-backed batch allocation for high-frequency topics.

## Concurrency Model

MVP:
- Single-threaded dispatch core for determinism.
- Thread-safe bus APIs via internal recursive mutex (current implementation).

Later:
- MPSC queue optimization for worker threads.
- Optional dedicated bus thread for tool-heavy standalone mode.

## Failure Model

Status enum categories:
- invalid argument
- allocation failed
- queue full / backpressure
- timeout
- transport unavailable
- schema mismatch

Reliability policy (MVP):
- Local deferred/immediate paths: best effort with queue bounds.
- Request/reply: timeout + explicit error status.

## Observability

Built-in counters:
- published, delivered, dropped, retried, timed out
- queue high-water marks
- per-topic throughput

Optional debug hooks:
- trace callback per envelope
- topic-level sampling

Current implementation includes bus trace hook API (`cd_bus_set_trace_hook`)
with enqueue/dispatch/transport/reply-capture event emission.

## Security and Isolation Notes

For local IPC:
- bind to user-private paths.
- reject unknown protocol versions.
- enforce payload size limits per topic.

## Integration Example (Engine + Tooling)

- Embedded engine:
  - Gameplay systems use one `cd_bus_t`.
  - Frame loop calls `cd_bus_pump()` once per tick.
- Standalone map tool:
  - Local bus + IPC transport connection to broker.
  - Publishes `map.generated` events and listens for `asset.import.request`.

## API Surface (MVP Target)

- Context lifecycle:
  - `cd_context_init`
  - `cd_context_shutdown`
- Bus lifecycle:
  - `cd_bus_create`
  - `cd_bus_destroy`
  - `cd_bus_pump`
- Messaging:
  - `cd_publish`
  - `cd_send_command`
  - `cd_request_async`
  - `cd_poll_reply`
- Subscriptions:
  - `cd_subscribe`
  - `cd_unsubscribe`

Exact symbol names can be finalized during Phase 0 API review.

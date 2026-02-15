# conduit Project Plan

## Vision

Build a reusable C messaging subsystem that supports both embedded module communication and standalone tool communication without changing module-facing APIs.

## Delivery Strategy

Implement in phases, each with strict acceptance gates.

## Phase 0: Spec Freeze and API Skeleton

Deliverables:
- Freeze envelope fields and status enum set.
- Freeze MVP API signatures and ownership rules.
- Create repository scaffold:
  - `include/conduit/`
  - `src/`
  - `tests/`
  - CMake target `conduit`

Acceptance criteria:
- Public headers compile clean with `-Wall -Wextra -Wpedantic -Werror`.
- API docs define memory and threading expectations.

## Phase 1: In-Process Bus MVP

Deliverables:
- Bus creation/destruction and subscription registry.
- Deferred publish + `cd_bus_pump()` deterministic dispatch.
- Directed command delivery.
- Queue bounds + backpressure status codes.

Acceptance criteria:
- Deterministic ordering tests pass for repeated seeded runs.
- No leaks in unit tests.
- Clear behavior for queue overflow.

## Phase 2: Request/Reply and Timeouts

Deliverables:
- Correlation id generation and tracking table.
- Async request API + reply polling.
- Timeout handling integrated into pump/tick progression.

Acceptance criteria:
- Request/reply happy-path tests.
- Timeout tests with predictable expiry.
- No orphaned inflight entries after timeout.

## Phase 3: Engine-Style Integration Slice

Deliverables:
- Integrate `conduit` in a minimal embedded sample loop.
- Define initial engine/event topic catalog conventions.
- Add sample gameplay-like event flow:
  - `frame.begin`
  - `map.generated`
  - `entity.spawn.request`

Acceptance criteria:
- Sample app demonstrates deterministic frame dispatch.
- Topic catalog and naming conventions documented.

Status:
- Implemented in current branch via:
  - `apps/embedded_sample/main.c`
  - `docs/TOPIC_CATALOG.md`

## Phase 4: IPC Transport Adapter

Deliverables:
- `ipc_socket` transport adapter with protocol framing.
- Envelope serialization/deserialization.
- Size/version guards and transport error mapping.

Acceptance criteria:
- Two-process integration test can exchange events and request/reply.
- Protocol mismatch paths return explicit errors.
- Payload size limits enforced.

Status:
- Started with transport abstraction + in-process transport adapter:
  - `include/conduit/transport.h`
  - `include/conduit/transport_inproc.h`
  - `src/transport_inproc.c`
- Bus transport integration and cross-bus tests are in place as groundwork for
  IPC framing work.

## Phase 5: Broker/Bridge App

Deliverables:
- `apps/broker` process for routing across tool processes.
- Route table by topic and endpoint.
- Basic metrics output (published/delivered/dropped/timeouts).

Acceptance criteria:
- Multi-process smoke test with broker in middle.
- Broker restarts handled cleanly by reconnect logic.

## Phase 6: Hardening and Tooling

Deliverables:
- Soak/load tests with synthetic high-volume topics.
- Profiling and queue strategy tuning.
- Trace hooks and diagnostics endpoints.
- CI test matrix for macOS/Linux.

Acceptance criteria:
- Throughput and latency targets met (define per hardware baseline).
- No crash/leak regressions in stress suite.

## Test Matrix

- Unit:
  - envelope validation
  - subscription matching
  - queue and ordering behavior
  - timeout and error mapping
- Integration:
  - embedded single-process dispatch
  - two-process IPC routing
  - broker-mediated multi-process routing
- Reliability:
  - queue saturation
  - transport disconnect/reconnect
  - malformed payload/schema mismatch

## Risks and Mitigations

- Risk: Scope creep into distributed systems concerns.
  - Mitigation: keep MVP local-process + local-IPC only.
- Risk: Non-deterministic behavior from multithreading.
  - Mitigation: single-threaded pump core first, threaded ingress later.
- Risk: Schema churn breaks consumers.
  - Mitigation: schema ids + version checks in envelope contract.

## Definition of Done (Initial Release)

1. Embedded modules can communicate through pub/sub + command + request/reply.
2. Same module-side code works through IPC transport.
3. Deterministic dispatch mode validated with reproducible tests.
4. Broker-enabled standalone tooling demo works end-to-end.
5. Public API and docs are stable enough for first adopter modules.

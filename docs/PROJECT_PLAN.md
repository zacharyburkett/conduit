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
- IPC frame codec implemented with version/size guards:
  - `include/conduit/transport_ipc.h`
  - `src/transport_ipc.c`
- IPC socket adapter implemented with framed stream I/O:
  - `include/conduit/transport_ipc_socket.h`
  - `src/transport_ipc_socket.c`
- Socketpair integration tests added for event and request/reply exchange.
- Forked two-process integration test added for cross-process event +
  request/reply exchange over IPC socket transport.

## Phase 5: Broker/Bridge App

Deliverables:
- `apps/broker` process for routing across tool processes.
- Route table by topic and endpoint.
- Basic metrics output (published/delivered/dropped/timeouts).

Acceptance criteria:
- Multi-process smoke test with broker in middle.
- Broker restarts handled cleanly by reconnect logic.

Status:
- Started with broker executable:
  - `apps/broker/main.c`
- Broker currently routes:
  - events by topic route table (dynamic or `--routes-file` explicit mapping)
  - command/request/reply by endpoint route table
- Basic broker metrics are emitted:
  - published/delivered/dropped/timeouts/transport_errors
- Integration test added (broker process in the middle):
  - `tests/test_main.c` (`test_broker_process_routes_between_clients`)
  - test auto-skips when the runtime disallows Unix socket path bind.
- Reconnect integration test added:
  - `tests/test_main.c` (`test_broker_reconnect_without_restart`)
- Broker restart + client reconnect integration test added:
  - `tests/test_main.c` (`test_broker_restart_with_client_reconnect`)
- Broker integration tests now assert broker final metrics output snapshots.

## Phase 6: Hardening and Tooling

Deliverables:
- Soak/load tests with synthetic high-volume topics.
- Profiling and queue strategy tuning.
- Trace hooks and diagnostics endpoints.
- CI test matrix for macOS/Linux.

Acceptance criteria:
- Throughput and latency targets met (define per hardware baseline).
- No crash/leak regressions in stress suite.

Status:
- Started with synthetic load harness:
  - `apps/loadgen/main.c`
- Soak integration test added:
  - `tests/test_main.c` (`test_loadgen_soak_against_broker`)
- Malformed-frame burst stress test added:
  - `tests/test_main.c` (`test_broker_malformed_frame_burst_under_load`)
- Disconnect storm stress test added:
  - `tests/test_main.c` (`test_broker_disconnect_storm_under_load`)
- Diagnostics endpoint start added:
  - reserved broker request/reply endpoint (`0xFFFFFFFE`)
  - metrics snapshot text payload reply (`clients/published/delivered/dropped/timeouts/transport_errors`)
  - integration test `tests/test_main.c` (`test_broker_diagnostics_request_endpoint`)
- Profiling/queue-tuning baseline started:
  - loadgen now emits phase timing, request RTT, and queue-pressure counters
  - loadgen now supports queue/inflight tuning flags
  - integration test `tests/test_main.c` (`test_loadgen_profile_baseline_against_broker`)
  - current conservative baseline gates:
    - throughput >= 200 msg/s
    - request avg RTT <= 50 ms
    - request max RTT <= 500 ms

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

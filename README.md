# conduit

`conduit` is a proposed messaging subsystem for your C toolchain/engine stack.

It is designed to support two modes without changing module-level message logic:
- Embedded mode: multiple modules in one process (single engine binary).
- Standalone mode: modules running as separate tools/processes.

## Docs

- Proposal: `docs/PROPOSAL.md`
- Architecture: `docs/ARCHITECTURE.md`
- Delivery plan: `docs/PROJECT_PLAN.md`
- API contract: `docs/API_CONTRACT.md`
- Topic catalog: `docs/TOPIC_CATALOG.md`
- Threading contract: `docs/THREADING.md`

## Build

```sh
cmake -S . -B build
cmake --build build
```

## Test

```sh
ctest --test-dir build --output-on-failure
```

## CI Matrix (Phase 6 Start)

GitHub Actions workflow:

- `.github/workflows/conduit-ci.yml`

Current CI behavior:

- OS matrix: `ubuntu-latest`, `macos-latest`
- Runs: configure, build, `ctest`
- Captures loadgen profile artifacts per OS with:
  - `scripts/ci/capture_loadgen_profile.sh`
  - `summary.txt`, `loadgen.log`, `broker.log`

## Embedded Sample

Build includes the sample app by default (`CONDUIT_BUILD_SAMPLES=ON`):

```sh
./build/conduit_embedded_sample
```

This sample demonstrates deterministic embedded dispatch with the flow:
- `frame.begin`
- `map.generated`
- `entity.spawn.request` (request/reply)

## Broker App (Phase 5 Start)

Run the broker over a Unix domain socket:

```sh
./build/conduit_broker --socket /tmp/conduit-broker.sock
```

Optional flags:
- `--max-clients <n>`
- `--max-topic-routes <n>`
- `--max-endpoint-routes <n>`
- `--metrics-interval-ms <n>`
- `--run-ms <n>`
- `--routes-file <path>`
- `--trace`

Route file format:

```txt
topic <topic_id> <endpoint_id> [endpoint_id...]
```

Example:

```txt
topic 3501 901
topic 0x00020001 20 21
```

Test coverage includes broker reconnect and broker restart recovery scenarios.
Broker integration tests also validate broker final metrics output counters.

## Broker Diagnostics Endpoint (Phase 6)

The broker now exposes a reserved request/reply endpoint for runtime metrics:

- endpoint: `0xFFFFFFFE`
- request topic: `0x00B00001`
- reply topic: `0x00B00001`
- reply schema: `0x00B00001` version `1`

Reply payload is UTF-8 text with key/value counters:

```txt
clients=<n> published=<n> delivered=<n> dropped=<n> timeouts=<n> transport_errors=<n>
```

Integration coverage includes a direct request/reply diagnostics test against a
live broker process (`test_broker_diagnostics_request_endpoint`).

## Load Generator (Phase 6 Start)

Run a synthetic load pass against a running broker:

```sh
./build/conduit_loadgen --socket /tmp/conduit-broker.sock --events 5000 --requests 1000
```

Optional flags:
- `--payload-size <bytes>`
- `--request-timeout-ns <n>`
- `--max-duration-ms <n>`
- `--max-queued-messages <n>`
- `--max-inflight-requests <n>`
- `--connect-attempts <n>`
- `--trace`

Loadgen summary now includes:
- overall throughput (`throughput_msg_per_s`)
- event/request phase durations (`event_phase_ms`, `request_phase_ms`)
- request roundtrip latency stats (`req_rtt_avg_us`, `req_rtt_max_us`)
- queue pressure counters (`event_queue_full_retries`, `request_queue_full_retries`)

Integration coverage includes a profiling baseline test with conservative
thresholds (`test_loadgen_profile_baseline_against_broker`) in addition to soak
and reliability stress scenarios.

Reliability tests now include malformed-frame burst and disconnect-storm
scenarios under active load.

## Bus Trace Hooks (Phase 6 Start)

The bus now supports optional trace callbacks:

- API: `cd_bus_set_trace_hook`
- Event kinds:
  - `CD_TRACE_EVENT_ENQUEUE`
  - `CD_TRACE_EVENT_DISPATCH`
  - `CD_TRACE_EVENT_REPLY_CAPTURE`
  - `CD_TRACE_EVENT_TRANSPORT_SEND`
  - `CD_TRACE_EVENT_TRANSPORT_POLL`

Trace hooks are exercised by unit test
`test_trace_hook_reports_core_events`.

## Transport Layer (Phase 4 Start)

- Generic transport contract: `include/conduit/transport.h`
- In-process hub transport: `include/conduit/transport_inproc.h`
- IPC frame codec (serialization guards): `include/conduit/transport_ipc.h`
- IPC socket transport adapter: `include/conduit/transport_ipc_socket.h`
- Bus supports transport attach/detach + polling in `cd_bus_pump`.
- Test suite includes a forked two-process IPC socket roundtrip scenario.

## Direction Summary

- Build an in-process bus first (deterministic, low overhead, simple API).
- Add transport adapters second (IPC/network bridge) without changing module code.
- Use one message envelope contract for gameplay events and inter-module communication.
- Keep reliability semantics explicit (fire-and-forget, at-least-once, request/reply).

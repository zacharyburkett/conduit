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

## Build

```sh
cmake -S . -B build
cmake --build build
```

## Test

```sh
ctest --test-dir build --output-on-failure
```

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

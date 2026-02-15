# Conduit

Conduit is a C messaging layer for modular tools and game-engine modules.

It is designed for one message API in two runtime modes:
- Embedded mode: multiple modules inside one process.
- Standalone mode: modules as separate processes connected by transports/broker.

## What You Get Today

- Pub/sub events, directed commands, and async request/reply.
- Deterministic queue pumping (`cd_bus_pump`) for frame-style loops.
- Cross-bus transports:
  - in-process hub transport
  - IPC socket transport
- Broker app for multi-process routing over Unix sockets.
- Diagnostics endpoint, trace hooks, and load/stress tooling.
- Thread-safe bus and transport internals with documented lifecycle rules.

## Quick Start

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

Run the embedded sample:

```sh
./build/conduit_embedded_sample
```

Run broker + load generator:

```sh
./build/conduit_broker --socket /tmp/conduit-broker.sock
./build/conduit_loadgen --socket /tmp/conduit-broker.sock --events 5000 --requests 1000
```

## Typical Embedded Flow

1. Create `cd_context_t`.
2. Create `cd_bus_t` with capacities.
3. Register subscriptions.
4. Publish/send/request from systems.
5. Call `cd_bus_pump` from your update loop.
6. Poll request tokens and dispose replies.
7. Shutdown bus/context.

## Broker and Routing Notes

- Broker executable: `./build/conduit_broker`
- Socket option: `--socket <path>`
- Optional static route file format:

```txt
topic <topic_id> <endpoint_id> [endpoint_id...]
```

- Reserved broker diagnostics endpoint:
  - endpoint: `0xFFFFFFFE`
  - topic: `0x00B00001`
  - payload format:

```txt
clients=<n> published=<n> delivered=<n> dropped=<n> timeouts=<n> transport_errors=<n>
```

## Threading Summary

- Bus APIs are safe for concurrent ingress.
- `cd_bus_pump` is single-consumer at runtime (one active call at a time).
- `cd_bus_destroy` must not race with other bus API calls.

See the full threading contract in `docs/THREADING.md`.

## Docs Map

- Getting the big picture: `docs/PROPOSAL.md`
- System design: `docs/ARCHITECTURE.md`
- Concrete behavior guarantees: `docs/API_CONTRACT.md`
- Thread-safety guarantees: `docs/THREADING.md`
- Topic naming and reserved ids: `docs/TOPIC_CATALOG.md`
- Delivery history and phase status: `docs/PROJECT_PLAN.md`

## CI

Workflow: `.github/workflows/conduit-ci.yml`

- Matrix: `ubuntu-latest`, `macos-latest`
- Runs configure/build/tests
- Uploads loadgen profile artifacts (`summary.txt`, `loadgen.log`, `broker.log`)

# Conduit

Conduit is a C messaging layer for modular tools and engine/runtime systems.

> Stability notice
> This repository is pre-1.0 and not stable. APIs, wire behavior, and diagnostics output may change between commits.

## What Is Implemented

- Core library target: `conduit::conduit`
- Message patterns:
  - publish/subscribe events
  - directed commands
  - async request/reply
- Deterministic bus pumping (`cd_bus_pump`) for frame-driven loops
- In-process transport and IPC transport layers
- Broker executable for multi-process routing
- Load generation and embedded sample apps
- Threading/lifecycle contract documentation and test coverage

## Build

```sh
cmake -S . -B build
cmake --build build
```

Run tests:

```sh
ctest --test-dir build --output-on-failure
```

## Apps

When `CONDUIT_BUILD_SAMPLES=ON`:

- `./build/conduit_embedded_sample`
- `./build/conduit_broker --socket /tmp/conduit-broker.sock`
- `./build/conduit_loadgen --socket /tmp/conduit-broker.sock --events 5000 --requests 1000`

## CMake Options

- `CONDUIT_BUILD_TESTS=ON|OFF`
- `CONDUIT_BUILD_SAMPLES=ON|OFF`

## Consumer Integration

From source:

```cmake
add_subdirectory(/absolute/path/to/conduit ${CMAKE_BINARY_DIR}/_deps/conduit EXCLUDE_FROM_ALL)
target_link_libraries(my_target PRIVATE conduit::conduit)
```

Installed package export is also generated (`conduitTargets.cmake`).

Public umbrella include:

- `include/conduit/conduit.h`

## Docs

- `docs/PROPOSAL.md`
- `docs/ARCHITECTURE.md`
- `docs/API_CONTRACT.md`
- `docs/THREADING.md`
- `docs/TOPIC_CATALOG.md`
- `docs/PROJECT_PLAN.md`

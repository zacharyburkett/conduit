# Conduit Proposal

This document captures the original design decision path for Conduit.

## Problem Statement

Your projects are evolving into a modular C engine ecosystem where components should run in two ways:

1. Standalone tools (separate processes).
2. Embedded modules (single executable/engine binary).

Without a shared messaging contract, modules become tightly coupled through direct function calls and ad hoc data handoff, which slows reuse and integration.

## Goals

- One communication model for both embedded and standalone use.
- Support gameplay events and system-level module communication.
- Keep deterministic behavior available for simulation/gameplay paths.
- Keep the C API small, explicit, and allocator-aware.
- Make integration incremental across existing modules (starting with `dungeoneer`-adjacent workflows).

## Non-Goals (Initial Milestone)

- Cross-machine distributed reliability guarantees.
- Exactly-once delivery semantics.
- Runtime schema reflection or dynamic scripting integration.

## Options Considered

### Option A: Direct module calls + callbacks

Pros:
- Minimal implementation cost.

Cons:
- Tight coupling.
- Hard to reuse same modules in standalone mode.
- No consistent observability or routing policy.

### Option B: Single in-process event bus only

Pros:
- Good for embedded engine workflows.
- Low overhead.

Cons:
- No clean bridge to standalone tools/processes.

### Option C: Full broker-first architecture from day one

Pros:
- Handles process boundaries immediately.

Cons:
- High complexity up front.
- Slower time-to-value for current in-process engine needs.

## Recommended Architecture

Adopt a layered model:

1. Core message API and in-process bus (first).
2. Transport abstraction layer (second).
3. Optional broker/bridge process for standalone tools (third).

This gives immediate value to embedded workflows while preserving a path to process-separated tools.

## Core Design Decisions

- Use a common message envelope for all message classes:
  - Events (pub/sub)
  - Commands (directed)
  - Requests/Replies (correlated)
- Make dispatch mode explicit:
  - Deferred/frame-safe dispatch for deterministic gameplay.
  - Immediate dispatch for tooling/control paths.
- Keep payload transport-agnostic:
  - Opaque bytes + schema/version identifiers.
- Keep ownership explicit:
  - Sender owns source payload memory.
  - Bus/transport owns copied envelope + payload while queued.
- Keep status/error handling C-style enum based (like your current libraries).

## Tradeoffs

- Copying payloads into queues is safer and transport-friendly, but costs memory bandwidth.
- Deterministic frame dispatch improves reproducibility, but introduces latency vs immediate callbacks.
- A transport abstraction adds design overhead, but prevents rewrite when adding standalone-process support.

## Success Criteria

- Modules can publish/subscribe in-process with deterministic ordering.
- Command and request/reply patterns work without global singletons.
- Same module code can run against in-process transport and IPC bridge transport.
- End-to-end tests prove message ordering, routing, and timeout behavior.

## Recommendation

Proceed with a staged rollout where the first production target is an embedded in-process bus that later gains IPC bridging for standalone tools. This keeps risk low while aligning with your engine trajectory.

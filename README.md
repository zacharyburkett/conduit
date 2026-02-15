# conduit

`conduit` is a proposed messaging subsystem for your C toolchain/engine stack.

It is designed to support two modes without changing module-level message logic:
- Embedded mode: multiple modules in one process (single engine binary).
- Standalone mode: modules running as separate tools/processes.

## Docs

- Proposal: `docs/PROPOSAL.md`
- Architecture: `docs/ARCHITECTURE.md`
- Delivery plan: `docs/PROJECT_PLAN.md`

## Direction Summary

- Build an in-process bus first (deterministic, low overhead, simple API).
- Add transport adapters second (IPC/network bridge) without changing module code.
- Use one message envelope contract for gameplay events and inter-module communication.
- Keep reliability semantics explicit (fire-and-forget, at-least-once, request/reply).

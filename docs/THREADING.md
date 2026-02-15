# Threading Contract (Phase 6 Start)

This document defines current thread-safety guarantees for `conduit`.

## Current Guarantee

- Bus internals are protected by an internal recursive mutex.
- The following APIs are thread-safe for concurrent calls on the same bus:
  - `cd_publish`
  - `cd_send_command`
  - `cd_request_async`
  - `cd_send_reply`
  - `cd_poll_reply`
  - `cd_reply_dispose`
  - `cd_subscribe`
  - `cd_unsubscribe`
  - `cd_bus_attach_transport`
  - `cd_bus_detach_transport`
  - `cd_bus_set_trace_hook`
  - `cd_bus_pump`

## Determinism Notes

- Dispatch callback execution remains single-consumer via `cd_bus_pump`.
- `cd_bus_pump` can be called from any thread, but one call executes at a time.
- Callback-triggered reentrant publish/request/reply is supported.

## Lifecycle Note

- `cd_bus_destroy` must not race with any other bus API calls.
- Caller is responsible for external shutdown coordination before destroy.

## Portability

- No SDL dependency is required.
- Concurrency is implemented via an internal platform abstraction:
  - pthread mutex (POSIX)
  - critical section (Windows)

# conduit API Contract (Phase 2)

This document freezes the initial API behavior and ownership rules for the scaffolding milestone.

## Threading Model

- Current implementation is single-threaded at bus dispatch.
- `cd_bus_pump` must be called from one thread at a time.
- Concurrent publish/subscribe is not yet guaranteed thread-safe.

## Memory Ownership

- Callers own input payload memory passed into `cd_publish` and `cd_send_command`.
- The bus copies payload bytes into queue-owned memory during enqueue.
- Subscriber callbacks receive read-only payload pointers (`cd_envelope_t.payload`).
- Message payload memory is released by the bus after dispatch completes.

## Delivery Semantics (Current)

- `cd_publish` enqueues an event; delivery happens when `cd_bus_pump` is called.
- Events are delivered to all subscribers that match topic and kind mask.
- Commands are delivered to subscribers matching topic, kind mask, and target endpoint.
- Subscription callback invocation order is deterministic by subscription registration id.
- `cd_bus_pump` drains a stable queue snapshot taken at call start.
- Messages published during callback execution are deferred to a later pump call.
- Requests are directed messages routed to the target endpoint.
- Replies are correlated through `correlation_id` (request `message_id`) and
  stored in the requester's inflight mailbox.
- Non-`LOCAL_ONLY` messages are forwarded to attached transports.
- Incoming transport messages are enqueued locally and processed by `cd_bus_pump`.

## Status and Error Behavior

- `CD_STATUS_INVALID_ARGUMENT`: null pointers or invalid required fields.
- `CD_STATUS_ALLOCATION_FAILED`: memory allocation failure.
- `CD_STATUS_QUEUE_FULL`: enqueue attempt beyond queue capacity.
- `CD_STATUS_CAPACITY_REACHED`: subscription or inflight-request table full.
- `CD_STATUS_NOT_FOUND`: unsubscribe unknown subscription id.
- `CD_STATUS_TIMEOUT`: request expired before a matching reply was captured.

## Request/Reply Semantics

- `cd_request_async` enqueues a `CD_MESSAGE_REQUEST` and returns a request token.
- `cd_send_reply` enqueues `CD_MESSAGE_REPLY` with request correlation id.
- Inflight request capacity is bounded by `cd_bus_config_t.max_inflight_requests`.
- `cd_poll_reply` is terminal for a request token:
  - `CD_STATUS_OK` + `out_ready=0`: still waiting
  - `CD_STATUS_OK` + `out_ready=1`: reply ready and token consumed
  - `CD_STATUS_TIMEOUT` + `out_ready=1`: timed out and token consumed
  - `CD_STATUS_NOT_FOUND`: token does not exist/already consumed
- Reply payload returned by `cd_poll_reply` must be released with
  `cd_reply_dispose`.

## Transport Semantics

- `cd_bus_attach_transport` stores non-owning transport references.
- `cd_bus_pump` polls attached transports before processing the local queue snapshot.
- Replies from transport are captured through normal inflight correlation.
- Timed-out requests are dropped if later encountered in the dispatch queue.
- IPC socket transport uses framed envelopes over stream sockets.
- Socket disconnect/read-write failures map to `CD_STATUS_TRANSPORT_UNAVAILABLE`.
- IPC frame/protocol violations map to `CD_STATUS_SCHEMA_MISMATCH`.
- Cross-process event and request/reply flow is validated with a forked
  two-process integration test.

## IPC Frame Codec Semantics

- `cd_ipc_encode_envelope` writes a fixed 64-byte header plus payload bytes.
- `cd_ipc_decode_envelope` validates protocol magic, version, header size, and
  frame length against encoded payload size.
- Protocol/frame mismatches return `CD_STATUS_SCHEMA_MISMATCH`.
- Payloads above configured codec limits return `CD_STATUS_CAPACITY_REACHED`.
- Decoded payload pointers reference the input frame memory.

## Broker MVP Semantics (Phase 5 Start)

- `apps/broker/main.c` provides a Unix-socket broker process.
- Event routing uses topic route entries:
  - default mode: fanout to connected peers
  - `--routes-file` mode: route by explicit `topic -> endpoint...` entries
- Command/request/reply routing uses endpoint-to-client route entries learned
  from source endpoints on incoming messages.
- Broker metrics currently expose published/delivered/dropped/timeouts and
  transport error counts.
- Route file line format is:
  - `topic <topic_id> <endpoint_id> [endpoint_id...]`
- Integration coverage includes reconnect and broker restart recovery paths.
- Integration tests also parse broker final metrics output and validate minimum
  expected counters.

## Load/Soak Harness Semantics (Phase 6 Start)

- `apps/loadgen/main.c` drives high-volume event and request/reply traffic
  through the broker.
- Loadgen validates message counts and reply correlation under bounded runtime.
- Integration test `test_loadgen_soak_against_broker` validates broker behavior
  under synthetic load.

## Phase Boundaries

- Socket transport framing is available; broker/process supervision is still a
  later milestone.

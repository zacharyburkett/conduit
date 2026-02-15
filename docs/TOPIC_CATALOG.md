# Topic Catalog (Phase 3 Sample)

This catalog defines initial topic and endpoint conventions used by the embedded sample app.

## Naming Convention

- Topic names use `domain.action` style.
- Topic ids are stable numeric constants in code.
- Schema ids and versions are explicit in every message envelope.

## Topics

- `frame.begin` (`0x00010001`)
  - Kind: `CD_MESSAGE_EVENT`
  - Producer: frame loop (`CD_ENDPOINT_FRAME_LOOP`)
  - Consumers: map system (`CD_ENDPOINT_MAP_SYSTEM`)
  - Payload schema: `CD_SCHEMA_FRAME_BEGIN_V1`

- `map.generated` (`0x00010002`)
  - Kind: `CD_MESSAGE_EVENT`
  - Producer: map system (`CD_ENDPOINT_MAP_SYSTEM`)
  - Consumers: gameplay system (`CD_ENDPOINT_GAMEPLAY_SYSTEM`)
  - Payload schema: `CD_SCHEMA_MAP_GENERATED_V1`

- `entity.spawn.request` (`0x00020001`)
  - Kind: `CD_MESSAGE_REQUEST` + `CD_MESSAGE_REPLY`
  - Request producer: gameplay system (`CD_ENDPOINT_GAMEPLAY_SYSTEM`)
  - Request consumer: entity service (`CD_ENDPOINT_ENTITY_SERVICE`)
  - Request payload schema: `CD_SCHEMA_ENTITY_SPAWN_REQUEST_V1`
  - Reply payload schema: `CD_SCHEMA_ENTITY_SPAWN_REPLY_V1`

## Endpoint Convention

- `CD_ENDPOINT_FRAME_LOOP = 1`
- `CD_ENDPOINT_MAP_SYSTEM = 10`
- `CD_ENDPOINT_GAMEPLAY_SYSTEM = 11`
- `CD_ENDPOINT_ENTITY_SERVICE = 20`

Endpoint ids are module/system identities and should be unique within a local bus instance.

#include "conduit/conduit.h"

const char *cd_status_string(cd_status_t status)
{
    switch (status) {
    case CD_STATUS_OK:
        return "ok";
    case CD_STATUS_INVALID_ARGUMENT:
        return "invalid argument";
    case CD_STATUS_ALLOCATION_FAILED:
        return "allocation failed";
    case CD_STATUS_QUEUE_FULL:
        return "queue full";
    case CD_STATUS_CAPACITY_REACHED:
        return "capacity reached";
    case CD_STATUS_NOT_FOUND:
        return "not found";
    case CD_STATUS_TIMEOUT:
        return "timeout";
    case CD_STATUS_TRANSPORT_UNAVAILABLE:
        return "transport unavailable";
    case CD_STATUS_SCHEMA_MISMATCH:
        return "schema mismatch";
    case CD_STATUS_NOT_IMPLEMENTED:
        return "not implemented";
    default:
        return "unknown status";
    }
}

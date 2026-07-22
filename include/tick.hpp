#pragma once
#include <cstdint>

// Fixed-size, POD, trivially-copyable struct so we can send it over the
// wire as raw bytes with no serialization step. Packed to avoid compiler
// padding differences causing mismatches between sender/receiver (in this
// project both sides are compiled together, but this is good practice for
// any binary wire protocol).
#pragma pack(push, 1)
struct Tick {
    uint64_t seq;            // monotonically increasing sequence number
    uint64_t gen_timestamp_ns; // timestamp captured when generated (producer side)
    double price;
    int32_t symbol_id;
};
#pragma pack(pop)

static_assert(sizeof(Tick) == 28, "Tick layout changed - check wire format");

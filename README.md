# Low-Latency Market Data Pipeline

A C++ simulation of the core market-data-to-execution path found in real algorithmic trading systems — built from scratch with no external dependencies beyond the standard library and Winsock2.

---

## What It Does

The project implements two complete, runnable pipelines that mirror how a real trading firm's infrastructure processes exchange data:

**Pipeline 1 — TCP Feed (simple baseline)**
```
market_data_generator  ──TCP──►  execution_engine
   (simulated exchange)              ├── network thread  ──push()──►  SpscQueue<Tick>
                                     │     (socket reader)               (lock-free)
                                     └── worker thread   ◄──pop()───  SpscQueue<Tick>
                                           (latency recorder + disk logger)
```

**Pipeline 2 — UDP Feed with Gap Recovery (realistic)**
```
feed_publisher  ──UDP──►  feed_handler
   (5% drops,                ├── network thread  (select() on UDP + TCP sockets)
    5% reorders,             │     detects gaps → sends TCP retransmission requests
    TCP retransmit server)   └── FeedSequencer  →  SpscQueue  →  worker thread
                                   (reorder buffer, timeout handling)
```

---

## Architecture & Design Decisions

### Lock-Free SPSC Queue ([`include/spsc_queue.hpp`](include/spsc_queue.hpp))

A hand-written single-producer / single-consumer ring buffer using `std::atomic` head and tail indices.

- **No mutexes, no CAS loops** — since exactly one thread produces and one consumes, plain `acquire`/`release` atomic stores are sufficient and cheaper than compare-and-swap
- **Cache-line isolation** — `head_` and `tail_` are each placed on their own 64-byte cache line with `alignas(64)`. Without this, every `push()` would invalidate the cache line holding `tail_` on the consumer core and vice versa (false sharing)
- **Power-of-two capacity** — enables bitmask indexing (`& mask_`) instead of modulo, removing a division from the hot path
- **Compile-time capacity** — `static_assert` enforces power-of-two at build time

### Thread Separation

The network thread and worker thread are always kept separate. The socket-reading thread must never block on business logic, and the strategy/processing thread must never block on the kernel waiting for the next packet. This is a standard pattern in production trading systems.

### UDP Market Data + TCP Retransmission ([`src/feed_publisher.cpp`](src/feed_publisher.cpp), [`src/feed_handler.cpp`](src/feed_handler.cpp))

Mirrors the real exchange feed model:

- **UDP** for the data feed: lower overhead, no per-client connection state, naturally supports multicast
- **TCP** for the retransmission channel: reliability is required when requesting specific missed sequence numbers — this is the out-of-band control path

The publisher intentionally drops 5% of ticks silently and delays another 5%, then makes them available for retransmission via a bounded circular replay buffer (16,384 slots).

### Gap Detection & Recovery ([`include/feed_sequencer.hpp`](include/feed_sequencer.hpp))

`FeedSequencer` is the core of the subscriber — deliberately separated from socket I/O so it can be unit-tested without any network setup:

1. Expects monotonically increasing sequence numbers starting at 1
2. On receiving seq N when expecting seq M (N > M): buffers N in a `std::map` reorder buffer, marks M..N-1 as missing, fires a TCP retransmission request
3. On receiving a retransmitted tick: processes it through the same sequencer path, flushing the reorder buffer if the gap is now filled
4. **Recovery timeout (50ms)**: if a missing sequence isn't recovered within the window, it is declared unrecoverable — the sequencer skips it, flushes any buffered subsequent ticks, and moves on. The system never blocks forever on a permanently dropped packet

### TCP_NODELAY

Set on every socket that carries latency-sensitive data. Disables Nagle's algorithm, which would otherwise buffer small packets and wait for an ACK before sending — adding unnecessary milliseconds when you're trying to measure microseconds.

### Buffered Binary I/O Logger

`BufferedTickLogger` batches 512 `Tick` structs per `write()` call rather than one syscall per tick. The goal is to move `write()` off the critical path: each OS `write()` is expensive; the runtime buffer is cheap.

### Latency Profiler ([`include/latency_profiler.hpp`](include/latency_profiler.hpp))

Records nanosecond-precision timestamps at generation time (producer side) and at processing time (consumer side). Reports **min / avg / p50 / p99 / max** in microseconds at shutdown. The `feed_handler` additionally separates latency samples for recovered (retransmitted) ticks vs. normal ticks.

### Wire Format ([`include/tick.hpp`](include/tick.hpp))

```cpp
#pragma pack(push, 1)
struct Tick {
    uint64_t seq;              // monotonically increasing sequence number
    uint64_t gen_timestamp_ns; // nanosecond timestamp captured at send time
    double   price;
    int32_t  symbol_id;
};
#pragma pack(pop)
static_assert(sizeof(Tick) == 28, "Tick layout changed - check wire format");
```

`#pragma pack(1)` eliminates compiler padding so the struct layout is identical on both sides of the socket. The `static_assert` is a build-time guard — if anyone changes the struct, the build breaks before a silent wire-format mismatch can corrupt data.

---

## Build

Requires [MSYS2](https://www.msys2.org/) (Windows, no WSL needed).

```bash
# Install toolchain (once)
pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-cmake mingw-w64-x86_64-make

# Build
mkdir build && cd build
cmake -G "MinGW Makefiles" ..
mingw32-make
```

Produces: `market_data_generator.exe`, `execution_engine.exe`, `feed_publisher.exe`, `feed_handler.exe`, `test_feed_handler.exe`, `test_spsc_queue.exe`

---

## Run

### TCP Pipeline

```bash
# Terminal 1
./market_data_generator.exe

# Terminal 2 (after generator prints "listening on port 5555")
./execution_engine.exe
```

`execution_engine` prints a full latency report and writes `processed_ticks.bin` on completion.

### UDP Pipeline with Gap Recovery

```bash
# Terminal 1
./feed_publisher.exe

# Terminal 2
./feed_handler.exe
```

`feed_handler` prints two latency reports: one for all ticks end-to-end, and one specifically for ticks that had to be recovered via retransmission.

### Visualize Output

```bash
python plot_ticks.py
# writes market_data_plot.png
```

---

## Tests

```bash
./test_feed_handler.exe   # FeedSequencer unit tests (no network required)
./test_spsc_queue.exe     # SPSC queue stress test: 1M items, cross-thread, asserts FIFO
```

`test_feed_handler` covers four behavioral cases:

| Test | What it verifies |
|---|---|
| `test_in_order` | Sequential ticks pass through immediately |
| `test_out_of_order` | Out-of-order tick triggers retransmission request; reorder buffer flushes on arrival |
| `test_unrecoverable_gap` | Gap that exceeds timeout is skipped; subsequent ticks still delivered |
| `test_duplicate_delivery` | Duplicate sequence number is silently dropped |

---

## Project Structure

```
trading-pipeline/
├── include/
│   ├── tick.hpp              # POD wire format (packed, static_assert guarded)
│   ├── spsc_queue.hpp        # Lock-free SPSC ring buffer
│   ├── latency_profiler.hpp  # Nanosecond latency recorder (min/avg/p50/p99/max)
│   └── feed_sequencer.hpp    # Gap detection, reorder buffer, timeout logic
├── src/
│   ├── market_data_generator.cpp  # TCP server: streams 100k ticks
│   ├── execution_engine.cpp       # TCP client: network+worker threads, SPSC queue
│   ├── feed_publisher.cpp         # UDP feed with artificial drops/reorders + TCP retransmit server
│   └── feed_handler.cpp           # UDP subscriber: gap detection, retransmission, recovery
├── tests/
│   ├── test_feed_handler.cpp  # FeedSequencer unit tests
│   └── test_spsc_queue.cpp    # SPSC queue cross-thread stress test
└── CMakeLists.txt
```

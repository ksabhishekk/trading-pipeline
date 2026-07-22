# Low-Latency Order Processing Pipeline

A small C++ project simulating the core shape of a trading system's
market-data-to-execution path: a TCP feed, a lock-free queue between
threads, and end-to-end latency measurement.

## Architecture

```
market_data_generator (TCP server)
        |  TCP, TCP_NODELAY
        v
execution_engine (TCP client)
   ├── network thread  --push()-->  SpscQueue<Tick>  --pop()-->  worker thread (main)
   │   (reads socket)                (lock-free,                 (measures latency,
   │                                   cache-line padded)          buffered binary log)
```

- **`market_data_generator`**: acts as a simulated exchange feed. Accepts one
  TCP connection and streams 100,000 fixed-size `Tick` structs, each
  timestamped at send time.
- **`execution_engine`**: connects to the generator. A dedicated network
  thread reads ticks off the socket and pushes them into a lock-free
  single-producer/single-consumer (SPSC) queue. The main thread pops from
  the queue, records latency from generation to processing, and writes
  processed ticks to disk via a buffered binary logger.

## Why these design choices

- **Lock-free SPSC queue (`include/spsc_queue.hpp`)**: uses `std::atomic`
  head/tail indices instead of a mutex. Since exactly one thread ever
  produces and one thread ever consumes, we can avoid CAS loops entirely —
  plain atomic loads/stores with acquire/release ordering are sufficient.
  `head_` and `tail_` are placed on separate cache lines (`alignas(64)`) to
  avoid false sharing between the producer and consumer cores.
- **TCP with `TCP_NODELAY`**: disables Nagle's algorithm so ticks are sent
  immediately instead of being buffered/coalesced by the OS, which matters
  once you're measuring microsecond-level latency.
- **Separate network vs. worker threads**: mirrors real trading system
  design — the socket-reading thread should never block on business logic,
  and business logic should never block on the kernel for the next packet.
- **Buffered binary I/O logger**: batches writes (default 512 records) into
  one `write()` call instead of one syscall per tick, reducing I/O overhead.
- **Latency profiler**: records nanosecond timestamps at generation and at
  processing, then reports min/avg/p50/p99/max at shutdown.

## Build (Windows, native — no WSL required)

1. Install [MSYS2](https://www.msys2.org/).
2. Open the **MINGW64** shell and install the toolchain:
   ```
   pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-cmake mingw-w64-x86_64-make
   ```
3. Add `C:\msys64\mingw64\bin` to your PATH.
4. From the project root:
   ```
   mkdir build && cd build
   cmake -G "MinGW Makefiles" ..
   mingw32-make
   ```

## Run

Open two terminals from the `build` directory:

```
# Terminal 1
./market_data_generator.exe

# Terminal 2 (after generator prints "listening on port 5555")
./execution_engine.exe
```

`execution_engine` will print a latency report (min/avg/p50/p99/max in
microseconds) once the generator finishes streaming, and will write
processed ticks to `processed_ticks.bin`.

## Test

```
./test_spsc_queue.exe
```

Pushes/pops 1,000,000 items across producer/consumer threads and asserts
strict FIFO order with zero drops or duplicates.

## Known limitations / what I'd do differently for production

- This runs over loopback TCP on a single machine, so absolute latency
  numbers reflect a dev laptop, not colo'd trading hardware — the point of
  this project is the *design*, not the raw numbers.
- A production system would likely use UDP multicast for market data (lower
  overhead, no per-client connection state) rather than TCP.
- The queue uses a busy-yield retry on full/empty rather than blocking —
  appropriate for a latency-sensitive hot path, but wastes CPU if left
  running idle; a production system might use a hybrid spin-then-park
  strategy.
- Correctness testing here is a hand-rolled assert-based test; would prefer
  Google Test in a longer-lived project for better reporting/CI integration.

## Market Data Feed Handler

The project now includes a simulated UDP market data feed publisher (`feed_publisher`) and subscriber (`feed_handler`), implementing sequence gap detection and recovery mechanisms.

### Why UDP for Market Data and TCP for Control?
- **UDP (User Datagram Protocol)** is used for the market data feed because it has lower overhead, supports multicast (sending to many clients at once), and doesn't require the exchange to maintain connection state for thousands of subscribers. However, UDP provides **no delivery guarantees** — packets can be dropped or arrive out of order.
- **TCP (Transmission Control Protocol)** is used for the retransmission request channel because **reliability is required** here. When a client detects a gap, it sends a control request for specific sequence numbers. The exchange must guarantee the delivery of these missed ticks, making TCP the appropriate choice for this out-of-band request/response flow.

### Gap Detection and Recovery Architecture
1. **Publisher Replay Buffer**: The `feed_publisher` maintains a bounded circular buffer (e.g., the last 16,384 ticks). It cannot store infinite history; real exchanges also limit replay availability (often capping it to the current day or a recent time window).
2. **Subscriber Gap Detection**: The `feed_handler` expects monotonically increasing sequence numbers. If an early sequence arrives (e.g., expected 5, received 7), it stores tick 7 in a **reorder buffer**, marks 5 and 6 as missing, and sends a TCP request to the publisher to resend the gap.
3. **Recovery Timeout**: The subscriber implements a bounded wait time (e.g., 50ms) for missed packets. If the missing packets are not recovered within this window, they are considered an "unrecoverable gap". The subscriber logs the missing sequence, increments its expected sequence number, flushes any queued out-of-order packets, and moves on. This ensures the system does not block forever on a permanently dropped packet.

### Known Limitations / Production Differences
- **Unicast vs Multicast**: This simulation uses unicast UDP (`127.0.0.1`) for simplicity. A production environment uses UDP Multicast (e.g., IGMP) allowing many machines to subscribe to a single stream without burdening the publisher network stack.
- **Standard Protocols**: We use a custom packed `Tick` struct. Real exchanges use vendor-specific binary protocols like ITCH, OUCH, or standard protocols like FIX/FAST.

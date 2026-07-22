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

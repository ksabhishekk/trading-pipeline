// Execution Engine (TCP client)
//
// Connects to the market data generator, and splits work across two threads:
//   1. Network thread (producer): reads Ticks off the TCP socket as fast as
//      they arrive and pushes them into a lock-free SPSC queue.
//   2. Worker thread (consumer): pops Ticks off the queue and "processes"
//      them (in a real system this would be strategy logic / order
//      generation; here we just simulate light work) while recording
//      end-to-end latency from generation to processing.
//
// This separation mirrors a real trading system design: you never want your
// socket-reading thread blocked on business logic, and you never want your
// strategy logic blocked waiting on the kernel for the next packet.
//
// Windows / Winsock2 build (native Windows, no WSL required).

#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <thread>
#include <atomic>
#include <fstream>
#include <cstring>
#include <vector>
#include <string>

#include "tick.hpp"
#include "spsc_queue.hpp"
#include "latency_profiler.hpp"

#pragma comment(lib, "Ws2_32.lib")

constexpr const char* SERVER_IP = "127.0.0.1";
constexpr int PORT = 5555;
constexpr std::size_t QUEUE_CAPACITY = 1 << 16; // must be power of two

static SpscQueue<Tick, QUEUE_CAPACITY> g_queue;
static std::atomic<bool> g_producer_done{false};

// Buffered binary writer for processed ticks.
// Writing one small struct at a time with raw ofstream::write() still goes
// through the C++ runtime's own internal buffering, but we additionally
// batch writes ourselves (flush every N records) to cut down the number of
// underlying OS write() calls, which is the actual expensive part.
class BufferedTickLogger {
public:
    explicit BufferedTickLogger(const std::string& path, std::size_t batch_size = 512)
        : out_(path, std::ios::binary), batch_size_(batch_size) {
        buffer_.reserve(batch_size_);
    }

    void write(const Tick& t) {
        buffer_.push_back(t);
        if (buffer_.size() >= batch_size_) {
            flush();
        }
    }

    void flush() {
        if (!buffer_.empty()) {
            out_.write(reinterpret_cast<const char*>(buffer_.data()),
                       static_cast<std::streamsize>(buffer_.size() * sizeof(Tick)));
            buffer_.clear();
        }
    }

    ~BufferedTickLogger() {
        flush();
    }

private:
    std::ofstream out_;
    std::vector<Tick> buffer_;
    std::size_t batch_size_;
};

void network_thread_fn(SOCKET sock) {
    Tick tick{};
    const int tick_size = static_cast<int>(sizeof(Tick));

    while (true) {
        int total_received = 0;
        // TCP is a byte stream, not a message stream -- a single recv() call
        // is NOT guaranteed to return a full Tick. We loop until we have
        // exactly sizeof(Tick) bytes.
        while (total_received < tick_size) {
            int r = recv(sock,
                          reinterpret_cast<char*>(&tick) + total_received,
                          tick_size - total_received, 0);
            if (r <= 0) {
                g_producer_done.store(true, std::memory_order_release);
                return; // connection closed or error
            }
            total_received += r;
        }

        // Busy-retry push if the queue is momentarily full rather than
        // dropping the tick. In a real system you'd size the queue and
        // consumer speed so this almost never happens.
        while (!g_queue.push(tick)) {
            std::this_thread::yield();
        }
    }
}

int main() {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed\n";
        return 1;
    }

    SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCKET) {
        std::cerr << "socket() failed: " << WSAGetLastError() << "\n";
        WSACleanup();
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, SERVER_IP, &server_addr.sin_addr);

    std::cout << "[engine] connecting to generator at " << SERVER_IP << ":" << PORT << "...\n";

    if (connect(sock, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == SOCKET_ERROR) {
        std::cerr << "connect() failed: " << WSAGetLastError()
                  << " (is market_data_generator running?)\n";
        closesocket(sock);
        WSACleanup();
        return 1;
    }

    BOOL nodelay = TRUE;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
               reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));

    std::cout << "[engine] connected. receiving ticks...\n";

    LatencyProfiler profiler;
    BufferedTickLogger logger("processed_ticks.bin");

    std::thread net_thread(network_thread_fn, sock);

    // Worker/consumer loop runs on main thread.
    Tick tick{};
    uint64_t processed_count = 0;
    while (true) {
        if (g_queue.pop(tick)) {
            uint64_t processed_ns = LatencyProfiler::now_ns();
            profiler.record(tick.gen_timestamp_ns, processed_ns);
            logger.write(tick);
            ++processed_count;
        } else if (g_producer_done.load(std::memory_order_acquire) && g_queue.empty()) {
            break; // producer finished and queue drained
        } else {
            std::this_thread::yield();
        }
    }

    net_thread.join();
    logger.flush();

    std::cout << "[engine] processed " << processed_count << " ticks\n";
    profiler.report("generator -> execution engine (end-to-end)");

    closesocket(sock);
    WSACleanup();
    return 0;
}

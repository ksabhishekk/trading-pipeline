// Market Data Feed Handler (Subscriber)
//
// Receives UDP feed, detects gaps, requests retransmissions over TCP,
// reorders ticks, and pushes them sequentially to a downstream queue.

#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <thread>
#include <atomic>
#include <fstream>
#include <vector>
#include <string>

#include "tick.hpp"
#include "spsc_queue.hpp"
#include "latency_profiler.hpp"
#include "feed_sequencer.hpp"

#pragma comment(lib, "Ws2_32.lib")

constexpr const char* SERVER_IP = "127.0.0.1";
constexpr int UDP_PORT = 5556;
constexpr int TCP_PORT = 5557;
constexpr std::size_t QUEUE_CAPACITY = 1 << 16;

static SpscQueue<Tick, QUEUE_CAPACITY> g_queue;
static std::atomic<bool> g_network_done{false};

struct RetransmissionRequest {
    uint64_t start_seq;
    uint64_t end_seq;
};

void network_thread_fn() {
    SOCKET udp_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (udp_sock == INVALID_SOCKET) return;

    BOOL opt = TRUE;
    setsockopt(udp_sock, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));

    sockaddr_in udp_addr{};
    udp_addr.sin_family = AF_INET;
    udp_addr.sin_addr.s_addr = INADDR_ANY;
    udp_addr.sin_port = htons(UDP_PORT);

    if (bind(udp_sock, reinterpret_cast<sockaddr*>(&udp_addr), sizeof(udp_addr)) == SOCKET_ERROR) {
        closesocket(udp_sock);
        return;
    }

    SOCKET tcp_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (tcp_sock == INVALID_SOCKET) {
        closesocket(udp_sock);
        return;
    }

    sockaddr_in tcp_addr{};
    tcp_addr.sin_family = AF_INET;
    tcp_addr.sin_port = htons(TCP_PORT);
    inet_pton(AF_INET, SERVER_IP, &tcp_addr.sin_addr);

    while (connect(tcp_sock, reinterpret_cast<sockaddr*>(&tcp_addr), sizeof(tcp_addr)) == SOCKET_ERROR) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    BOOL nodelay = TRUE;
    setsockopt(tcp_sock, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));

    auto on_tick = [](Tick t) {
        while (!g_queue.push(t)) {
            std::this_thread::yield();
        }
    };

    auto on_request = [tcp_sock](uint64_t start, uint64_t end) {
        RetransmissionRequest req{start, end};
        send(tcp_sock, reinterpret_cast<const char*>(&req), sizeof(req), 0);
    };

    FeedSequencer sequencer(on_tick, on_request);

    u_long mode = 1;
    ioctlsocket(udp_sock, FIONBIO, &mode);
    ioctlsocket(tcp_sock, FIONBIO, &mode);

    Tick tick;
    int tick_size = sizeof(Tick);
    
    int tcp_total_received = 0;
    Tick tcp_tick;

    while (true) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(udp_sock, &read_fds);
        FD_SET(tcp_sock, &read_fds);

        timeval tv{0, 1000}; // 1ms timeout

        int ready = select(0, &read_fds, nullptr, nullptr, &tv);
        if (ready == SOCKET_ERROR) break;

        uint64_t now = LatencyProfiler::now_ns();

        if (ready > 0 && FD_ISSET(udp_sock, &read_fds)) {
            sockaddr_in sender{};
            int sender_len = sizeof(sender);
            int r = recvfrom(udp_sock, reinterpret_cast<char*>(&tick), tick_size, 0,
                             reinterpret_cast<sockaddr*>(&sender), &sender_len);
            if (r == tick_size) {
                sequencer.process_tick(tick, now);
            }
        }

        if (ready > 0 && FD_ISSET(tcp_sock, &read_fds)) {
            int r = recv(tcp_sock, reinterpret_cast<char*>(&tcp_tick) + tcp_total_received,
                         tick_size - tcp_total_received, 0);
            if (r > 0) {
                tcp_total_received += r;
                if (tcp_total_received == tick_size) {
                    sequencer.process_tick(tcp_tick, now);
                    tcp_total_received = 0;
                }
            } else if (r == 0 || (r == SOCKET_ERROR && WSAGetLastError() != WSAEWOULDBLOCK)) {
                break; // Publisher disconnected or error
            }
        }

        sequencer.check_timeouts(LatencyProfiler::now_ns());
    }

    g_network_done.store(true, std::memory_order_release);
    closesocket(udp_sock);
    closesocket(tcp_sock);
}

class BufferedTickLogger {
public:
    explicit BufferedTickLogger(const std::string& path, std::size_t batch_size = 512)
        : out_(path, std::ios::binary), batch_size_(batch_size) {
        buffer_.reserve(batch_size_);
    }

    void write(const Tick& t) {
        buffer_.push_back(t);
        if (buffer_.size() >= batch_size_) flush();
    }

    void flush() {
        if (!buffer_.empty()) {
            out_.write(reinterpret_cast<const char*>(buffer_.data()),
                       static_cast<std::streamsize>(buffer_.size() * sizeof(Tick)));
            buffer_.clear();
        }
    }

    ~BufferedTickLogger() { flush(); }

private:
    std::ofstream out_;
    std::vector<Tick> buffer_;
    std::size_t batch_size_;
};

int main() {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed\n";
        return 1;
    }

    std::cout << "[handler] starting network thread...\n";
    std::thread net_thread(network_thread_fn);

    LatencyProfiler end_to_end_profiler;
    LatencyProfiler recovery_profiler;
    BufferedTickLogger logger("processed_ticks.bin");

    Tick tick{};
    uint64_t processed_count = 0;
    
    while (true) {
        if (g_queue.pop(tick)) {
            uint64_t processed_ns = LatencyProfiler::now_ns();
            uint64_t latency = processed_ns - tick.gen_timestamp_ns;
            
            end_to_end_profiler.record(tick.gen_timestamp_ns, processed_ns);
            
            // If latency > 5ms, assume it was recovered
            if (latency > 5000000) {
                recovery_profiler.record(tick.gen_timestamp_ns, processed_ns);
            }
            
            logger.write(tick);
            ++processed_count;
        } else if (g_network_done.load(std::memory_order_acquire) && g_queue.empty()) {
            break;
        } else {
            std::this_thread::yield();
        }
    }

    net_thread.join();
    logger.flush();

    std::cout << "[handler] processed " << processed_count << " ticks\n";
    end_to_end_profiler.report("generator -> execution engine (end-to-end)");
    recovery_profiler.report("gap recovery (retransmitted ticks)");

    WSACleanup();
    return 0;
}

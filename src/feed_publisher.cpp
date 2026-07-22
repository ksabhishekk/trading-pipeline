// Market Data Feed Publisher (Simulated Exchange)
//
// Simulates an exchange feed over UDP. Sends Tick messages with artificial
// packet loss and out-of-order delivery.
// Runs a secondary TCP server for retransmission requests.

#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <random>
#include <atomic>
#include <mutex>

#include "tick.hpp"
#include "latency_profiler.hpp"

#pragma comment(lib, "Ws2_32.lib")

constexpr const char* UDP_IP = "127.0.0.1";
constexpr int UDP_PORT = 5556;
constexpr int TCP_PORT = 5557;
constexpr int NUM_TICKS = 100000;
constexpr size_t REPLAY_CAPACITY = 16384; // must be power of two
constexpr size_t REPLAY_MASK = REPLAY_CAPACITY - 1;

// Replay buffer to store recently sent ticks
Tick g_replay_buffer[REPLAY_CAPACITY];
std::atomic<uint64_t> g_latest_seq{0};
std::mutex g_replay_mutex;

struct RetransmissionRequest {
    uint64_t start_seq;
    uint64_t end_seq;
};

// TCP listener for retransmission requests
void retransmission_thread_fn() {
    SOCKET listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listen_sock == INVALID_SOCKET) return;

    BOOL opt = TRUE;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(TCP_PORT);

    if (bind(listen_sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
        closesocket(listen_sock);
        return;
    }

    if (listen(listen_sock, SOMAXCONN) == SOCKET_ERROR) {
        closesocket(listen_sock);
        return;
    }

    std::cout << "[publisher] TCP retransmission listener ready on port " << TCP_PORT << "\n";

    while (true) {
        SOCKET client_sock = accept(listen_sock, nullptr, nullptr);
        if (client_sock == INVALID_SOCKET) continue;
        
        BOOL nodelay = TRUE;
        setsockopt(client_sock, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));

        // Processing requests on this thread for simplicity
        std::thread([client_sock]() {
            RetransmissionRequest req;
            while (true) {
                int r = recv(client_sock, reinterpret_cast<char*>(&req), sizeof(req), 0);
                if (r <= 0) break; // Client disconnected or error
                
                if (r == sizeof(req)) {
                    uint64_t current_latest = g_latest_seq.load(std::memory_order_acquire);
                    std::vector<Tick> resend_ticks;
                    
                    {
                        std::lock_guard<std::mutex> lock(g_replay_mutex);
                        for (uint64_t s = req.start_seq; s <= req.end_seq; ++s) {
                            if (s <= current_latest && (current_latest - s) < REPLAY_CAPACITY) {
                                Tick t = g_replay_buffer[s & REPLAY_MASK];
                                if (t.seq == s) {
                                    resend_ticks.push_back(t);
                                }
                            }
                        }
                    }

                    for (const auto& t : resend_ticks) {
                        send(client_sock, reinterpret_cast<const char*>(&t), sizeof(Tick), 0);
                    }
                }
            }
            closesocket(client_sock);
        }).detach();
    }
}

int main() {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed\n";
        return 1;
    }

    std::thread tcp_thread(retransmission_thread_fn);
    tcp_thread.detach();

    SOCKET udp_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (udp_sock == INVALID_SOCKET) {
        std::cerr << "socket() failed: " << WSAGetLastError() << "\n";
        WSACleanup();
        return 1;
    }

    sockaddr_in dest_addr{};
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_port = htons(UDP_PORT);
    inet_pton(AF_INET, UDP_IP, &dest_addr.sin_addr);

    std::cout << "[publisher] starting UDP feed to " << UDP_IP << ":" << UDP_PORT << " in 2 seconds...\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));

    std::mt19937 rng(42);
    std::uniform_real_distribution<double> dist(0.0, 1.0);

    double price = 100.0;
    std::vector<Tick> delayed_queue;

    for (uint64_t i = 1; i <= NUM_TICKS; ++i) { // Sequences start at 1
        Tick tick{};
        tick.seq = i;
        tick.symbol_id = 1;
        price += ((i % 7 == 0) ? 0.05 : -0.02);
        tick.price = price;
        tick.gen_timestamp_ns = LatencyProfiler::now_ns();

        {
            std::lock_guard<std::mutex> lock(g_replay_mutex);
            g_replay_buffer[i & REPLAY_MASK] = tick;
        }
        g_latest_seq.store(i, std::memory_order_release);

        double rand_val = dist(rng);
        if (rand_val < 0.05) {
            // 5% chance to silently drop
            // std::cout << "[publisher] artificially dropping seq " << i << "\n";
            continue; 
        } else if (rand_val < 0.10) {
            // 5% chance to delay
            // std::cout << "[publisher] artificially delaying seq " << i << "\n";
            delayed_queue.push_back(tick);
            continue;
        }

        sendto(udp_sock, reinterpret_cast<const char*>(&tick), sizeof(Tick), 0,
               reinterpret_cast<const sockaddr*>(&dest_addr), sizeof(dest_addr));

        if (!delayed_queue.empty() && dist(rng) < 0.3) {
            for (const auto& t : delayed_queue) {
                sendto(udp_sock, reinterpret_cast<const char*>(&t), sizeof(Tick), 0,
                       reinterpret_cast<const sockaddr*>(&dest_addr), sizeof(dest_addr));
            }
            delayed_queue.clear();
        }

        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }

    for (const auto& t : delayed_queue) {
        sendto(udp_sock, reinterpret_cast<const char*>(&t), sizeof(Tick), 0,
               reinterpret_cast<const sockaddr*>(&dest_addr), sizeof(dest_addr));
    }

    std::cout << "[publisher] done sending ticks.\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));

    closesocket(udp_sock);
    WSACleanup();
    return 0;
}

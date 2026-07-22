// Market Data Generator (TCP server)
//
// Simulates an exchange feed: accepts one connection from the execution
// engine, then streams a configurable number of Tick messages over TCP as
// fast as possible (a small sleep can be added to simulate a realistic
// feed rate). Each tick is timestamped at the moment it's sent so the
// consumer side can measure end-to-end latency later.
//
// Windows / Winsock2 build (native Windows, no WSL required).

#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <thread>
#include <chrono>

#include "tick.hpp"
#include "latency_profiler.hpp"

#pragma comment(lib, "Ws2_32.lib")

constexpr int PORT = 5555;
constexpr int NUM_TICKS = 100000;

int main() {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed\n";
        return 1;
    }

    SOCKET listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listen_sock == INVALID_SOCKET) {
        std::cerr << "socket() failed: " << WSAGetLastError() << "\n";
        WSACleanup();
        return 1;
    }

    // Allow quick restart during dev without "address already in use".
    BOOL opt = TRUE;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR,
               reinterpret_cast<const char*>(&opt), sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    if (bind(listen_sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
        std::cerr << "bind() failed: " << WSAGetLastError() << "\n";
        closesocket(listen_sock);
        WSACleanup();
        return 1;
    }

    if (listen(listen_sock, 1) == SOCKET_ERROR) {
        std::cerr << "listen() failed: " << WSAGetLastError() << "\n";
        closesocket(listen_sock);
        WSACleanup();
        return 1;
    }

    std::cout << "[generator] listening on port " << PORT
              << " ... waiting for execution engine to connect\n";

    SOCKET client_sock = accept(listen_sock, nullptr, nullptr);
    if (client_sock == INVALID_SOCKET) {
        std::cerr << "accept() failed: " << WSAGetLastError() << "\n";
        closesocket(listen_sock);
        WSACleanup();
        return 1;
    }

    std::cout << "[generator] client connected, streaming " << NUM_TICKS << " ticks\n";

    // Disable Nagle's algorithm: for a latency-sensitive feed we want each
    // tick to go out immediately, not get buffered/coalesced by the OS.
    BOOL nodelay = TRUE;
    setsockopt(client_sock, IPPROTO_TCP, TCP_NODELAY,
               reinterpret_cast<const char*>(&nodelay), sizeof(nodelay));

    double price = 100.0;
    for (uint64_t i = 0; i < NUM_TICKS; ++i) {
        Tick tick{};
        tick.seq = i;
        tick.symbol_id = 1;
        price += ((i % 7 == 0) ? 0.05 : -0.02); // fake price walk
        tick.price = price;
        tick.gen_timestamp_ns = LatencyProfiler::now_ns();

        int sent = send(client_sock, reinterpret_cast<const char*>(&tick), sizeof(Tick), 0);
        if (sent != static_cast<int>(sizeof(Tick))) {
            std::cerr << "[generator] send() failed at seq " << i
                      << " (" << WSAGetLastError() << ")\n";
            break;
        }
    }

    std::cout << "[generator] done sending, closing connection\n";

    closesocket(client_sock);
    closesocket(listen_sock);
    WSACleanup();
    return 0;
}

#pragma once
#include <vector>
#include <cstdint>
#include <algorithm>
#include <iostream>
#include <chrono>

// Simple, non-thread-safe latency recorder.
// Call record() once per processed message on the consumer thread,
// then call report() once at shutdown to print min/max/avg/p99.
//
// NOTE: This is intentionally simple (a growing vector + a sort at the end)
// rather than something fancy like a HDR histogram. That tradeoff is fine
// for a benchmark tool that reports once at shutdown, but would NOT be fine
// on the actual hot path of a production system -- worth being able to
// explain that distinction if asked.
class LatencyProfiler {
public:
    explicit LatencyProfiler(std::size_t reserve = 100000) {
        samples_ns_.reserve(reserve);
    }

    void record(uint64_t start_ns, uint64_t end_ns) {
        if (end_ns >= start_ns) {
            samples_ns_.push_back(end_ns - start_ns);
        }
    }

    void report(const std::string& label) const {
        if (samples_ns_.empty()) {
            std::cout << "[" << label << "] no samples recorded\n";
            return;
        }

        std::vector<uint64_t> sorted = samples_ns_;
        std::sort(sorted.begin(), sorted.end());

        const std::size_t n = sorted.size();
        const uint64_t min_ns = sorted.front();
        const uint64_t max_ns = sorted.back();
        const uint64_t p50 = sorted[n * 50 / 100];
        const uint64_t p99 = sorted[std::min(n - 1, n * 99 / 100)];

        long double sum = 0;
        for (auto v : sorted) sum += static_cast<long double>(v);
        const long double avg_ns = sum / static_cast<long double>(n);

        auto to_us = [](long double ns) { return ns / 1000.0L; };

        std::cout << "\n=== Latency Report: " << label << " ===\n";
        std::cout << "samples : " << n << "\n";
        std::cout << "min     : " << to_us(min_ns) << " us\n";
        std::cout << "avg     : " << to_us(avg_ns) << " us\n";
        std::cout << "p50     : " << to_us(p50) << " us\n";
        std::cout << "p99     : " << to_us(p99) << " us\n";
        std::cout << "max     : " << to_us(max_ns) << " us\n";
    }

    static uint64_t now_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
    }

private:
    std::vector<uint64_t> samples_ns_;
};

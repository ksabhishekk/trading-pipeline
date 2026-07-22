// Correctness test for SpscQueue: one producer thread pushes a known
// sequence of integers, one consumer thread pops them, and we verify
// every value arrives exactly once and in order, with none dropped.
//
// NOTE: this is a plain assert-based test, not Google Test. If you have
// time left, this is trivial to port to Google Test (gtest) by installing
// it via MSYS2 (`pacman -S mingw-w64-x86_64-gtest`) and wrapping each
// check below in a TEST(...) macro instead of assert(). Kept dependency-free
// here so it builds with zero extra setup under time pressure.

#include <cassert>
#include <thread>
#include <iostream>
#include <atomic>

#include "spsc_queue.hpp"

constexpr int NUM_ITEMS = 1'000'000;

int main() {
    SpscQueue<int, 1024> queue;
    std::atomic<bool> producer_done{false};

    std::thread producer([&]() {
        for (int i = 0; i < NUM_ITEMS; ++i) {
            while (!queue.push(i)) {
                std::this_thread::yield();
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::thread consumer([&]() {
        int expected = 0;
        int value = 0;
        while (expected < NUM_ITEMS) {
            if (queue.pop(value)) {
                assert(value == expected && "items must arrive in FIFO order, none dropped/duplicated");
                ++expected;
            } else if (producer_done.load(std::memory_order_acquire) && queue.empty()) {
                break;
            } else {
                std::this_thread::yield();
            }
        }
        assert(expected == NUM_ITEMS && "consumer must receive every item the producer pushed");
    });

    producer.join();
    consumer.join();

    std::cout << "test_spsc_queue: PASSED (" << NUM_ITEMS << " items, FIFO order preserved, none dropped)\n";
    return 0;
}

#pragma once
#include <atomic>
#include <cstddef>
#include <array>

// Lock-free Single-Producer Single-Consumer ring buffer.
//
// Design notes (worth remembering for an interview):
// - One thread ONLY ever calls push() (the producer), one thread ONLY ever
//   calls pop() (the consumer). This constraint is what lets us avoid CAS
//   loops entirely and just use plain atomic loads/stores with the right
//   memory ordering.
// - head_ is written only by the producer, read by both.
// - tail_ is written only by the consumer, read by both.
// - Capacity is a compile-time power of two so we can use a bitmask instead
//   of a modulo op (cheaper, and avoids a division on the hot path).
// - head_ and tail_ are placed on separate cache lines (alignas 64) so that
//   the producer writing head_ doesn't invalidate the consumer's cached copy
//   of tail_ and vice versa. Without this padding, the two atomics would
//   likely share a cache line and every push/pop would ping-pong that line
//   between CPU cores -- classic "false sharing".

template <typename T, std::size_t Capacity>
class SpscQueue {
    static_assert((Capacity & (Capacity - 1)) == 0,
                  "Capacity must be a power of two");

public:
    SpscQueue() : head_(0), tail_(0) {}

    // Producer side only. Returns false if the queue is full.
    bool push(const T& item) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t next_head = (head + 1) & mask_;

        // Acquire here so we see the consumer's latest tail_ write before
        // deciding whether we're full.
        if (next_head == tail_.load(std::memory_order_acquire)) {
            return false; // queue full
        }

        buffer_[head] = item;

        // Release so the write to buffer_[head] happens-before the consumer
        // observes the updated head_.
        head_.store(next_head, std::memory_order_release);
        return true;
    }

    // Consumer side only. Returns false if the queue is empty.
    bool pop(T& out) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);

        if (tail == head_.load(std::memory_order_acquire)) {
            return false; // queue empty
        }

        out = buffer_[tail];

        const std::size_t next_tail = (tail + 1) & mask_;
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    bool empty() const {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

private:
    static constexpr std::size_t mask_ = Capacity - 1;

    std::array<T, Capacity> buffer_;

    alignas(64) std::atomic<std::size_t> head_; // producer-owned
    alignas(64) std::atomic<std::size_t> tail_; // consumer-owned
};

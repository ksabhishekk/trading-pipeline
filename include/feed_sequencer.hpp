#pragma once
#include <map>
#include <vector>
#include <functional>
#include <cstdint>
#include "tick.hpp"
#include "latency_profiler.hpp"

// Pure logic for Gap Detection, Reordering, and Timeout handling.
// Separated from socket I/O for testability.
class FeedSequencer {
public:
    using OutputCallback = std::function<void(Tick)>;
    using RequestCallback = std::function<void(uint64_t, uint64_t)>;

    FeedSequencer(OutputCallback on_tick, RequestCallback on_request, uint64_t timeout_ns = 50000000)
        : on_tick_(on_tick), on_request_(on_request), timeout_ns_(timeout_ns), expected_seq_(1) {}

    void process_tick(Tick t, uint64_t now_ns) {
        if (t.seq < expected_seq_) return; // Already processed or skipped

        if (t.seq == expected_seq_) {
            on_tick_(t);
            expected_seq_++;

            while (reorder_buffer_.count(expected_seq_)) {
                on_tick_(reorder_buffer_[expected_seq_]);
                reorder_buffer_.erase(expected_seq_);
                expected_seq_++;
            }
        } else {
            reorder_buffer_[t.seq] = t;

            uint64_t gap_start = 0;
            bool in_gap = false;
            
            for (uint64_t s = expected_seq_; s < t.seq; ++s) {
                if (reorder_buffer_.count(s) == 0 && missing_seq_timeouts_.count(s) == 0) {
                    missing_seq_timeouts_[s] = now_ns + timeout_ns_;
                    if (!in_gap) { gap_start = s; in_gap = true; }
                } else if (in_gap) {
                    on_request_(gap_start, s - 1);
                    in_gap = false;
                }
            }
            if (in_gap) on_request_(gap_start, t.seq - 1);
        }
    }

    void check_timeouts(uint64_t now_ns) {
        while (!missing_seq_timeouts_.empty()) {
            auto it = missing_seq_timeouts_.begin();
            if (it->first < expected_seq_) {
                missing_seq_timeouts_.erase(it);
            } else if (it->first == expected_seq_) {
                if (now_ns >= it->second) {
                    // Unrecoverable gap
                    missing_seq_timeouts_.erase(it);
                    expected_seq_++;

                    while (reorder_buffer_.count(expected_seq_)) {
                        on_tick_(reorder_buffer_[expected_seq_]);
                        reorder_buffer_.erase(expected_seq_);
                        expected_seq_++;
                    }
                } else {
                    break; // Hasn't timed out yet
                }
            } else {
                break; // We only care if expected_seq_ is timed out
            }
        }
    }

    uint64_t get_expected_seq() const { return expected_seq_; }
    size_t get_buffer_size() const { return reorder_buffer_.size(); }
    size_t get_missing_count() const { return missing_seq_timeouts_.size(); }

private:
    OutputCallback on_tick_;
    RequestCallback on_request_;
    uint64_t timeout_ns_;
    
    std::map<uint64_t, Tick> reorder_buffer_;
    std::map<uint64_t, uint64_t> missing_seq_timeouts_;
    uint64_t expected_seq_;
};

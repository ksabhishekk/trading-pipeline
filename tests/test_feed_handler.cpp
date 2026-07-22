#include <iostream>
#include <vector>
#include <cassert>
#include "feed_sequencer.hpp"

void test_in_order() {
    std::vector<uint64_t> output;
    FeedSequencer seq([&](Tick t) { output.push_back(t.seq); },
                      [](uint64_t, uint64_t) {});

    for (uint64_t i = 1; i <= 5; ++i) {
        Tick t{}; t.seq = i;
        seq.process_tick(t, 0);
    }

    assert(output.size() == 5);
    for (uint64_t i = 0; i < 5; ++i) {
        assert(output[i] == i + 1);
    }
    std::cout << "test_in_order passed\n";
}

void test_out_of_order() {
    std::vector<uint64_t> output;
    int request_calls = 0;
    FeedSequencer seq([&](Tick t) { output.push_back(t.seq); },
                      [&](uint64_t, uint64_t) { request_calls++; });

    Tick t1{}; t1.seq = 1;
    Tick t2{}; t2.seq = 2;
    Tick t3{}; t3.seq = 3;

    seq.process_tick(t1, 0);
    assert(output.size() == 1);

    // Arrives out of order
    seq.process_tick(t3, 0);
    assert(output.size() == 1);
    assert(request_calls == 1); // Requested seq 2

    // Delayed packet arrives
    seq.process_tick(t2, 0);
    assert(output.size() == 3);
    assert(output[0] == 1 && output[1] == 2 && output[2] == 3);
    std::cout << "test_out_of_order passed\n";
}

void test_unrecoverable_gap() {
    std::vector<uint64_t> output;
    FeedSequencer seq([&](Tick t) { output.push_back(t.seq); },
                      [](uint64_t, uint64_t) {},
                      50); // 50ns timeout

    Tick t1{}; t1.seq = 1;
    Tick t3{}; t3.seq = 3;

    seq.process_tick(t1, 0);
    seq.process_tick(t3, 0); // gap of 2

    // Timeout hasn't passed
    seq.check_timeouts(10);
    assert(output.size() == 1);

    // Timeout passes
    seq.check_timeouts(60);
    
    assert(output.size() == 2);
    assert(output[0] == 1);
    assert(output[1] == 3); // 2 is skipped

    std::cout << "test_unrecoverable_gap passed\n";
}

void test_duplicate_delivery() {
    std::vector<uint64_t> output;
    FeedSequencer seq([&](Tick t) { output.push_back(t.seq); },
                      [](uint64_t, uint64_t) {});

    Tick t1{}; t1.seq = 1;

    seq.process_tick(t1, 0);
    seq.process_tick(t1, 0); // duplicate

    assert(output.size() == 1);
    std::cout << "test_duplicate_delivery passed\n";
}

int main() {
    std::cout << "Running FeedSequencer tests...\n";
    test_in_order();
    test_out_of_order();
    test_unrecoverable_gap();
    test_duplicate_delivery();
    std::cout << "All tests passed.\n";
    return 0;
}

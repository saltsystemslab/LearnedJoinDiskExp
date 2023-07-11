#ifndef BENCHMARK_LOOKUP_H
#define BENCHMARK_LOOKUP_H

#include "uniform_random.h"
#include "learned_lookup.h"
#include <chrono>

void lookup_learned_test(uint64_t num_queries,
    IteratorWithModel<Slice> *it, Comparator<Slice> *comp, int key_size) {

    vector<uint64_t> scan_test;
    for (uint64_t i=0; i<it->num_keys(); i++) {
        scan_test.push_back(i);
    }

    char q_copy[key_size];
    std::random_shuffle(scan_test.begin(), scan_test.end());
    auto scan_search_start = std::chrono::high_resolution_clock::now();
    while (num_queries) {
        for (auto pos: scan_test) {
            Slice q = it->peek(pos);
            memcpy(q_copy, q.data_, q.size_);
            Slice scan_query(q_copy, q.size_);
            LearnedLookup::lower_bound<Slice>(it, comp, scan_query);
            num_queries--;
            if (num_queries == 0) {
                break;
            }
        }
    }
    auto scan_search_end = std::chrono::high_resolution_clock::now();
    uint64_t scan_search_duration_ns = 
        std::chrono::duration_cast<std::chrono::nanoseconds>(scan_search_end - scan_search_start)
            .count();
    float scan_search_duration_sec = scan_search_duration_ns/1e9;
    printf("Lookup Duration: %lf\n", scan_search_duration_sec);
}

void lookup_binary_search(uint64_t num_queries,
    IteratorWithModel<Slice> *it, Comparator<Slice> *comp, int key_size) {
    vector<uint64_t> scan_test;
    for (uint64_t i=0; i<it->num_keys(); i++) {
        scan_test.push_back(i);
    }
    char q_copy[key_size];
    std::random_shuffle(scan_test.begin(), scan_test.end());
    auto scan_search_start = std::chrono::high_resolution_clock::now();
    while (num_queries) {
        for (auto pos: scan_test) {
            Slice q = it->peek(pos);
            memcpy(q_copy, q.data_, q.size_);
            Slice scan_query(q_copy, q.size_);
            StandardLookup::lower_bound<Slice>(it->get_iterator(), comp, scan_query);
            num_queries--;
            if (num_queries == 0) {
                break;
            }
        }
    }
    auto scan_search_end = std::chrono::high_resolution_clock::now();
    uint64_t scan_search_duration_ns = 
        std::chrono::duration_cast<std::chrono::nanoseconds>(scan_search_end - scan_search_start)
            .count();
    float scan_search_duration_sec = scan_search_duration_ns/1e9;
    printf("Lookup Duration: %lf\n", scan_search_duration_sec);
}

#endif
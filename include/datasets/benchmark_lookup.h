#ifndef BENCHMARK_LOOKUP_H
#define BENCHMARK_LOOKUP_H

#include "uniform_random.h"
#include "learned_lookup.h"
#include <chrono>

uint64_t lookup_learned_test_uniform(
    IteratorWithModel<Slice> *it, Comparator<Slice> *comp, 
    uint64_t num_queries, int key_size
) {
    char *c = new char[num_queries * key_size];
    fill_rand_bytes(c, key_size * num_queries);
    auto lookup_start = std::chrono::high_resolution_clock::now();
    for (uint64_t i=0; i<num_queries; i++) {
        Slice q(c + i*key_size, key_size);
        LearnedLookup::lower_bound<Slice>(it, comp, q);
    }
    auto lookup_end = std::chrono::high_resolution_clock::now();
    uint64_t lookup_duration_ns = 
        std::chrono::duration_cast<std::chrono::nanoseconds>(lookup_end - lookup_start)
            .count();
    float lookup_duration_sec = lookup_duration_ns/1e9;
    printf("Lookup Duration: %lf\n", lookup_duration_sec);

    auto scan_search_start = std::chrono::high_resolution_clock::now();
    it->seekToFirst();
    while(it->valid()) {
        LearnedLookup::lower_bound<Slice>(it, comp, it->key());
        it->next();
    }
    auto scan_search_end = std::chrono::high_resolution_clock::now();
    uint64_t scan_search_duration_ns = 
        std::chrono::duration_cast<std::chrono::nanoseconds>(scan_search_end - scan_search_start)
            .count();
    float scan_search_duration_sec = scan_search_duration_ns/1e9;
    printf("Scan Search: %lf\n", scan_search_duration_sec);

    return lookup_duration_ns;
}

uint64_t lookup_binary_search_uniform(
    IteratorWithModel<Slice> *it, Comparator<Slice> *comp, 
    uint64_t num_queries, int key_size
) {
    char *c = new char[num_queries * key_size];
    fill_rand_bytes(c, num_queries * key_size);
    auto lookup_start = std::chrono::high_resolution_clock::now();
    for (uint64_t i=0; i<num_queries; i++) {
        Slice q(c + i*key_size, key_size);
        StandardLookup::lower_bound<Slice>(it->get_iterator(), comp, q);
    }
    auto lookup_end = std::chrono::high_resolution_clock::now();
    uint64_t lookup_duration_ns = 
        std::chrono::duration_cast<std::chrono::nanoseconds>(lookup_end - lookup_start)
            .count();
    float lookup_duration_sec = lookup_duration_ns/1e9;
    printf("Lookup Duration: %lf\n", lookup_duration_sec);
    return lookup_duration_ns;
}

#endif
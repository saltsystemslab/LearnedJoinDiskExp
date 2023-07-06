#ifndef TEST_INPUT_H
#define TEST_INPUT_H

#include "iterator.h"
#include "iterator_with_model.h"

enum MergeMode {
  NO_OP,
  STANDARD_MERGE,
  PARALLEL_STANDARD_MERGE,
  PARALLEL_LEARNED_MERGE,
  PARALLEL_LEARNED_MERGE_BULK,
  MERGE_WITH_MODEL,
  MERGE_WITH_MODEL_BULK,
  STANDARD_MERGE_JOIN,
  STANDARD_MERGE_BINARY_LOOKUP_JOIN,
  LEARNED_MERGE_JOIN
};

template <class T>
struct BenchmarkInput {
  int num_of_lists;
  MergeMode merge_mode;
  IteratorWithModel<T> **iterators_with_model;
  Iterator<T> **iterators;
  IteratorBuilder<T> *resultBuilder;
  Comparator<T> *comparator;
  uint64_t total_input_keys_cnt;
  int key_size_bytes;
  bool is_parallel() {
    return (merge_mode == PARALLEL_LEARNED_MERGE_BULK ||
            PARALLEL_LEARNED_MERGE || PARALLEL_STANDARD_MERGE);
  }
  bool is_learned() {
    return (merge_mode == PARALLEL_LEARNED_MERGE_BULK ||
            PARALLEL_LEARNED_MERGE || MERGE_WITH_MODEL ||
            MERGE_WITH_MODEL_BULK || LEARNED_MERGE_JOIN);
  }
  bool is_join() {
    return (merge_mode == STANDARD_MERGE_JOIN || LEARNED_MERGE_JOIN);
  }
};

#endif
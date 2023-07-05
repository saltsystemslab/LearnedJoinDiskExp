#ifndef TEST_INPUT_H
#define TEST_INPUT_H

#include "iterator.h"
#include "iterator_with_model.h"

enum MERGE_MODE {
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

#if !USE_STRING_KEYS && USE_INT_128
std::string int128_to_string(KEY_TYPE k) {
  std::string s;
  while (k) {
    int d = (int)(k % 10);
    s = std::to_string(d) + s;
    k = k / 10;
  }
  return s;
}
#endif

#if !USE_STRING_KEYS && !USE_INT_128
std::string uint64_to_string(KEY_TYPE k) {
  std::string s;
  while (k) {
    int d = (int)(k % 10);
    s = std::to_string(d) + s;
    k = k / 10;
  }
  return s;
}
#endif

struct TestInput {
  int num_of_lists;
  MERGE_MODE merge_mode;
  IteratorWithModel<KEY_TYPE> **iterators_with_model;
  Iterator<KEY_TYPE> **iterators;
  IteratorBuilder<KEY_TYPE> *resultBuilder;
  Comparator<KEY_TYPE> *comparator;
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

  void print_input_data() {
    for (int i = 0; i < num_of_lists; i++) {
      Iterator<KEY_TYPE> *iter = iterators[i];
      iter->seekToFirst();
      printf("List %d\n", i);
      while (iter->valid()) {
#if USE_STRING_KEYS
        std::string key_str = iter->key().toString();
#elif USE_INT_128
        std::string key_str = int128_to_string(iter->key());
#else
        std::string key_str = uint64_to_string(iter->key());
#endif
        printf("%d Key: %lu %s\n", i, iter->current_pos(), key_str.c_str());
        iter->next();
      }
    }
  }
};



#endif
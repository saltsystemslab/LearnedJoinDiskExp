#ifndef TEST_INPUT_H
#define TEST_INPUT_H

#include "iterator.h"
#include "iterator_with_model.h"
#include "slice_plr_model.h"
#include "comparator.h"
#include <vector>

using namespace std;

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

struct BenchmarkInput {
  std::string test_dir;
  std::string result_file;

  int num_of_lists;
  vector<uint64_t> list_sizes;
  int key_size_bytes;
  int plr_error_bound;
  bool is_disk_backed;
  uint64_t num_common_keys;

  MergeMode merge_mode;
  IteratorWithModel<Slice> **iterators_with_model;
  Comparator<Slice> *comparator;
  SliceToPlrPointConverter *slice_to_point_converter;

  uint64_t total_input_keys_cnt;
  Iterator<Slice> **iterators;
  IteratorBuilder<Slice> *resultBuilder;
  bool is_parallel() {
    switch(merge_mode) {
      case PARALLEL_LEARNED_MERGE_BULK:
      case PARALLEL_LEARNED_MERGE:
      case PARALLEL_STANDARD_MERGE:
       return true;
    }
    return false;
  }

  bool is_learned() {
    switch(merge_mode) {
      case PARALLEL_LEARNED_MERGE_BULK:
      case PARALLEL_LEARNED_MERGE:
      case LEARNED_MERGE_JOIN:
      case MERGE_WITH_MODEL_BULK:
      case MERGE_WITH_MODEL:
       return true;
    }
    return false;
  }
  bool is_join() {
    return (merge_mode == STANDARD_MERGE_JOIN || merge_mode == LEARNED_MERGE_JOIN);
  }
};

#endif

#ifndef LEARNEDINDEXMERGE_JOIN_H
#define LEARNEDINDEXMERGE_JOIN_H

#include "comparator.h"
#include "index.h"
#include "iterator.h"
#include "partition.h"
#include "sstable.h"
#include "key_value_slice.h"
#include <thread>
#include <unordered_set>

namespace li_merge {

template <class T>
SSTable<T> *hash_join(std::unordered_map<std::string, uint64_t> *outer_index,
                      SSTable<T> *inner, SSTableBuilder<T> *result) {
  auto inner_iterator = inner->iterator();
  inner_iterator->seekToFirst();
  std::string prev;
  while (inner_iterator->valid()) {
    KVSlice kv = inner_iterator->key();
    std::string const key(kv.data(), kv.key_size_bytes());
    if (outer_index->find(key) != outer_index->end() && prev != key) {
      int repeats = outer_index->at(key);
      for (int i=0; i<repeats; i++)
        result->add(inner_iterator->key());
    }
    prev = key;
    inner_iterator->next();
  }
  return result->build();
}

template <class T>
SSTable<T> *indexed_nested_loop_join(SSTable<T> *outer, SSTable<T> *inner,
                                     Index<T> *inner_index,
                                     Comparator<T> *comparator,
                                     SSTableBuilder<T> *result) {
  auto outer_iterator = outer->iterator();
  auto inner_iterator = inner->iterator();
  outer_iterator->seekToFirst();
  inner_iterator->seekToFirst();
  uint64_t inner_num_elts = inner_iterator->num_elts();
  while (outer_iterator->valid()) {
    uint64_t approx_pos = inner_index->getApproxPosition(outer_iterator->key());
    approx_pos = std::min(approx_pos, inner_num_elts - 1);
    // Set to first value lesser than or equal to the inner key.
    bool is_overshoot = false;
    while (approx_pos && comparator->compare(inner_iterator->peek(approx_pos),
                                             outer_iterator->key()) > 0) {
      is_overshoot = true;
      approx_pos--;
    }
    // We could have undershot before, so go ahead till you meet key greater
    // than current key. If overshot, then there's no need to do this
    // correction.
    while (!is_overshoot && approx_pos < inner_iterator->num_elts() &&
           comparator->compare(inner_iterator->peek(approx_pos),
                               outer_iterator->key()) < 0) {
      approx_pos++;
    }

    // No more keys in larger list.
    if (approx_pos == inner_iterator->num_elts()) {
      break;
    }
    if (comparator->compare(inner_iterator->peek(approx_pos),
                            outer_iterator->key()) == 0) {
      result->add(outer_iterator->key());
    }
    outer_iterator->next();
  }
  return result->build();
}

template <class T>
SSTable<T> *presorted_merge_join(SSTable<T> *outer, SSTable<T> *inner,
                                 Comparator<T> *comparator,
                                 SSTableBuilder<T> *result_builder) {
  auto outer_iterator = outer->iterator();
  auto inner_iterator = inner->iterator();
  outer_iterator->seekToFirst();
  inner_iterator->seekToFirst();
  while (outer_iterator->valid()) {
    while (inner_iterator->valid() &&
           (comparator->compare(inner_iterator->key(), outer_iterator->key()) <
            0)) {
      inner_iterator->next();
    }
    if (!inner_iterator->valid()) {
      break;
    }
    if (comparator->compare(outer_iterator->key(), inner_iterator->key()) ==
        0) {
      result_builder->add(inner_iterator->key());
    }
    outer_iterator->next();
  }
  return result_builder->build();
}

template <class T>
SSTable<T> *parallel_hash_join(int num_threads, SSTable<T> *outer_table,
                               std::unordered_map<std::string, uint64_t> *outer_index,
                               SSTable<T> *inner_table, Index<T> *inner_index,
                               Comparator<T> *comparator,
                               PSSTableBuilder<T> *resultBuilder) {
  auto partition = partition_sstables(num_threads, outer_table, inner_table,
                                      inner_index, comparator);
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    uint64_t outer_start = partition[i].outer.first;
    uint64_t outer_end = partition[i].outer.second;
    uint64_t inner_start = partition[i].inner.first;
    uint64_t inner_end = partition[i].inner.second;
    threads.push_back(
        std::thread(hash_join<T>, outer_index,
                    inner_table->getSSTableForSubRange(inner_start, inner_end),
                    resultBuilder->getBuilderForRange(inner_start + outer_start,
                                                      inner_end + outer_end)));
  }
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }
  return resultBuilder->build();
}

template <class T>
SSTable<T> *parallel_indexed_nested_loop_join(
    int num_threads, SSTable<T> *outer_table, SSTable<T> *inner_table,
    Index<T> *inner_index, Comparator<T> *comparator,
    PSSTableBuilder<T> *resultBuilder) {
  auto partition = partition_sstables(num_threads, outer_table, inner_table,
                                      inner_index, comparator);
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    uint64_t outer_start = partition[i].outer.first;
    uint64_t outer_end = partition[i].outer.second;
    uint64_t inner_start = partition[i].inner.first;
    uint64_t inner_end = partition[i].inner.second;
    threads.push_back(std::thread(
        indexed_nested_loop_join<T>, outer_table,
                    inner_table->getSSTableForSubRange(inner_start, inner_end),
                    inner_index->getIndexForSubrange(inner_start, inner_end), comparator,
                    resultBuilder->getBuilderForRange(inner_start + outer_start,
                                                      inner_end + outer_end)));
  }
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }
  return resultBuilder->build();
}

template <class T>
SSTable<T> *parallel_presort_join(
    int num_threads, SSTable<T> *outer_table, SSTable<T> *inner_table,
    Index<T> *inner_index, Comparator<T> *comparator,
    PSSTableBuilder<T> *resultBuilder) {
  auto partition = partition_sstables(num_threads, outer_table, inner_table,
                                      inner_index, comparator);
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    uint64_t outer_start = partition[i].outer.first;
    uint64_t outer_end = partition[i].outer.second;
    uint64_t inner_start = partition[i].inner.first;
    uint64_t inner_end = partition[i].inner.second;
    threads.push_back(
        std::thread(presorted_merge_join<T>, outer_table,
                    inner_table->getSSTableForSubRange(inner_start, inner_end),
                    comparator,
                    resultBuilder->getBuilderForRange(inner_start + outer_start,
                                                      inner_end + outer_end)));
  }
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
  }
  return resultBuilder->build();
}


} // namespace li_merge

#endif
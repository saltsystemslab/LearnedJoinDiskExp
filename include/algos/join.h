#ifndef LEARNEDINDEXMERGE_JOIN_H
#define LEARNEDINDEXMERGE_JOIN_H

#include "comparator.h"
#include "index.h"
#include "iterator.h"
#include "key_value_slice.h"
#include "partition.h"
#include "sstable.h"
#include <thread>
#include <unordered_set>
// TODO(chesetti): Must be last for some reason. Fix.
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace li_merge {

template <class T>
SSTable<T> *hash_join(std::unordered_map<std::string, uint64_t> *outer_index,
                      SSTable<T> *inner, SSTableBuilder<T> *result,
                      json *join_stats) {
  auto inner_iterator = inner->iterator();
  inner_iterator->seekToFirst();
  std::string prev;
  while (inner_iterator->valid()) {
    KVSlice kv = inner_iterator->key();
    std::string const key(kv.data(), kv.key_size_bytes());
    if (outer_index->find(key) != outer_index->end() && prev != key) {
      int repeats = outer_index->at(key);
      for (int i = 0; i < repeats; i++)
        result->add(inner_iterator->key());
    }
    prev = key;
    inner_iterator->next();
  }
  (*join_stats)["inner_disk_fetch"] = inner_iterator->getDiskFetches();
  (*join_stats)["outer_disk_fetch"] = 0;
  return result->build();
}

template <class T>
SSTable<T> *indexed_nested_loop_join_windowed(
    SSTable<T> *outer, SSTable<T> *inner, Index<T> *inner_index,
    Comparator<T> *comparator, SSTableBuilder<T> *result, json *join_stats) {
  auto outer_iterator = outer->iterator();
  auto inner_iterator = inner->iterator(inner_index->getMaxError());
  outer_iterator->seekToFirst();
  inner_iterator->seekToFirst();
  uint64_t inner_num_elts = inner_iterator->numElts();
  while (outer_iterator->valid()) {
    uint64_t lower =
        inner_index->getPositionBounds(outer_iterator->key()).lower;
    lower = std::min(lower, inner_num_elts - 1);
    inner_iterator->peek(lower);
    if (inner_iterator->checkCache(outer_iterator->key())) {
      result->add(outer_iterator->key());
    }
    outer_iterator->next();
  }
  (*join_stats)["inner_disk_fetch"] = inner_iterator->getDiskFetches();
  (*join_stats)["outer_disk_fetch"] = outer_iterator->getDiskFetches();
  return result->build();
}

template <class T>
SSTable<T> *sort_merge_join(SSTable<T> *outer, SSTable<T> *inner,
                                 Comparator<T> *comparator,
                                 SSTableBuilder<T> *result_builder,
                                 json *join_stats) {
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
  (*join_stats)["inner_disk_fetch"] = inner_iterator->getDiskFetches();
  (*join_stats)["outer_disk_fetch"] = outer_iterator->getDiskFetches();
  return result_builder->build();
}

template <class T>
uint64_t lower_bound(Iterator<T> *iter, Comparator<T> *c, uint64_t lo,
                     uint64_t hi, T key) {
  // Last value that is lesser than or equal to lo
  while (lo < hi) {
    uint64_t mid = lo + (hi - lo + 1) / 2;
    if (c->compare(iter->peek(mid), key) <= 0) {
      lo = mid;
    } else {
      hi = mid - 1;
    }
  }
  return lo;
}

template <class T>
SSTable<T> *sort_merge_join_exponential_search(SSTable<T> *outer, SSTable<T> *inner,
                                     Comparator<T> *comparator,
                                     SSTableBuilder<T> *result_builder,
                                     json *join_stats) {
  auto outer_iterator = outer->iterator();
  auto inner_iterator = inner->iterator();
  outer_iterator->seekToFirst();
  inner_iterator->seekToFirst();
  uint64_t inner_nelts = inner_iterator->numElts();
  uint64_t inner_cur_pos = 0;
  while (outer_iterator->valid()) {
    uint64_t cur_pos = inner_cur_pos;
    uint64_t bound = 1;
    while (bound + cur_pos < inner_nelts &&
           comparator->compare(inner_iterator->peek(cur_pos + bound),
                               outer_iterator->key()) < 0) {
      bound = bound * 2;
    }
    uint64_t lim = std::min(inner_nelts - 1, cur_pos + bound);
    uint64_t next_pos = lower_bound(inner_iterator, comparator, cur_pos, lim,
                                    outer_iterator->key());
    inner_cur_pos = next_pos;
    if (comparator->compare(outer_iterator->key(),
                            inner_iterator->peek(next_pos)) == 0) {
      result_builder->add(outer_iterator->key());
    }
    outer_iterator->next();
  }
  (*join_stats)["inner_disk_fetch"] = inner_iterator->getDiskFetches();
  (*join_stats)["outer_disk_fetch"] = outer_iterator->getDiskFetches();
  return result_builder->build();
}

template <class T>
SSTable<T> *
parallel_hash_join(int num_threads, SSTable<T> *outer_table,
                   std::unordered_map<std::string, uint64_t> *outer_index,
                   SSTable<T> *inner_table, Index<T> *inner_index,
                   Comparator<T> *comparator, PSSTableBuilder<T> *resultBuilder,
                   json *join_stats) {
  auto partition = partition_sstables(num_threads, outer_table, inner_table,
                                      inner_index, comparator);
  std::vector<std::thread> threads;
  std::vector<json> join_stats_per_partition(num_threads);
  (*join_stats)["inner_disk_fetch"] = 0;
  (*join_stats)["outer_disk_fetch"] = 0;
  for (int i = 0; i < num_threads; i++) {
    uint64_t outer_start = partition[i].outer.first;
    uint64_t outer_end = partition[i].outer.second;
    uint64_t inner_start = partition[i].inner.first;
    uint64_t inner_end = partition[i].inner.second;
    threads.push_back(
        std::thread(hash_join<T>, outer_index,
                    inner_table->getSSTableForSubRange(inner_start, inner_end),
                    resultBuilder->getBuilderForRange(inner_start + outer_start,
                                                      inner_end + outer_end),
                    &join_stats_per_partition[i]));
  }
  uint64_t inner_disk_fetch_count = 0;
  uint64_t outer_disk_fetch_count = 0;
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
    inner_disk_fetch_count +=
        (uint64_t)join_stats_per_partition[i]["inner_disk_fetch"];
    outer_disk_fetch_count +=
        (uint64_t)join_stats_per_partition[i]["outer_disk_fetch"];
  }
  (*join_stats)["inner_disk_fetch"] = inner_disk_fetch_count;
  (*join_stats)["outer_disk_fetch"] = outer_disk_fetch_count;
  return resultBuilder->build();
}

template <class T>
SSTable<T> *parallel_indexed_nested_loop_join(
    int num_threads, SSTable<T> *outer_table, SSTable<T> *inner_table,
    Index<T> *inner_index, Comparator<T> *comparator,
    PSSTableBuilder<T> *resultBuilder, json *join_stats) {
  auto partition = partition_sstables(num_threads, outer_table, inner_table,
                                      inner_index, comparator);
  (*join_stats)["inner_disk_fetch"] = 0;
  (*join_stats)["outer_disk_fetch"] = 0;
  std::vector<std::thread> threads;
  std::vector<json> join_stats_per_partition(num_threads);
  for (int i = 0; i < num_threads; i++) {
    uint64_t outer_start = partition[i].outer.first;
    uint64_t outer_end = partition[i].outer.second;
    uint64_t inner_start = partition[i].inner.first;
    uint64_t inner_end = partition[i].inner.second;
    threads.push_back(std::thread(
        indexed_nested_loop_join_windowed<T>,
        outer_table->getSSTableForSubRange(outer_start, outer_end),
        inner_table->getSSTableForSubRange(inner_start, inner_end),
        inner_index->getIndexForSubrange(inner_start, inner_end), comparator,
        resultBuilder->getBuilderForRange(inner_start + outer_start,
                                          inner_end + outer_end),
        &join_stats_per_partition[i]));
  }
  uint64_t inner_disk_fetch_count = 0;
  uint64_t outer_disk_fetch_count = 0;
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
    inner_disk_fetch_count +=
        (uint64_t)join_stats_per_partition[i]["inner_disk_fetch"];
    outer_disk_fetch_count +=
        (uint64_t)join_stats_per_partition[i]["outer_disk_fetch"];
  }
  (*join_stats)["inner_disk_fetch"] = inner_disk_fetch_count;
  (*join_stats)["outer_disk_fetch"] = outer_disk_fetch_count;
  return resultBuilder->build();
}

template <class T>
SSTable<T> *
parallel_presort_join(int num_threads, SSTable<T> *outer_table,
                      SSTable<T> *inner_table, Index<T> *inner_index,
                      Comparator<T> *comparator,
                      PSSTableBuilder<T> *resultBuilder, json *join_stats) {
  auto partition = partition_sstables(num_threads, outer_table, inner_table,
                                      inner_index, comparator);
  std::vector<std::thread> threads;
  std::vector<json> join_stats_per_partition(num_threads);
  (*join_stats)["inner_disk_fetch"] = 0;
  (*join_stats)["outer_disk_fetch"] = 0;
  for (int i = 0; i < num_threads; i++) {
    uint64_t outer_start = partition[i].outer.first;
    uint64_t outer_end = partition[i].outer.second;
    uint64_t inner_start = partition[i].inner.first;
    uint64_t inner_end = partition[i].inner.second;
    threads.push_back(std::thread(
        sort_merge_join<T>,
        outer_table->getSSTableForSubRange(outer_start, outer_end),
        inner_table->getSSTableForSubRange(inner_start, inner_end), comparator,
        resultBuilder->getBuilderForRange(inner_start + outer_start,
                                          inner_end + outer_end),
        &join_stats_per_partition[i]));
  }
  uint64_t inner_disk_fetch_count = 0;
  uint64_t outer_disk_fetch_count = 0;
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
    inner_disk_fetch_count +=
        (uint64_t)join_stats_per_partition[i]["inner_disk_fetch"];
    outer_disk_fetch_count +=
        (uint64_t)join_stats_per_partition[i]["outer_disk_fetch"];
  }
  (*join_stats)["inner_disk_fetch"] = inner_disk_fetch_count;
  (*join_stats)["outer_disk_fetch"] = outer_disk_fetch_count;
  return resultBuilder->build();
}

template <class T>
SSTable<T> *
parallel_presort_join_exp(int num_threads, SSTable<T> *outer_table,
                          SSTable<T> *inner_table, Index<T> *inner_index,
                          Comparator<T> *comparator,
                          PSSTableBuilder<T> *resultBuilder, json *join_stats) {
  auto partition = partition_sstables(num_threads, outer_table, inner_table,
                                      inner_index, comparator);
  std::vector<std::thread> threads;
  std::vector<json> join_stats_per_partition(num_threads);
  (*join_stats)["inner_disk_fetch"] = 0;
  (*join_stats)["outer_disk_fetch"] = 0;
  for (int i = 0; i < num_threads; i++) {
    uint64_t outer_start = partition[i].outer.first;
    uint64_t outer_end = partition[i].outer.second;
    uint64_t inner_start = partition[i].inner.first;
    uint64_t inner_end = partition[i].inner.second;
    threads.push_back(std::thread(
        sort_merge_join_exponential_search<T>,
        outer_table->getSSTableForSubRange(outer_start, outer_end),
        inner_table->getSSTableForSubRange(inner_start, inner_end), comparator,
        resultBuilder->getBuilderForRange(inner_start + outer_start,
                                          inner_end + outer_end),
        &join_stats_per_partition[i]));
  }
  uint64_t inner_disk_fetch_count = 0;
  uint64_t outer_disk_fetch_count = 0;
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
    inner_disk_fetch_count +=
        (uint64_t)join_stats_per_partition[i]["inner_disk_fetch"];
    outer_disk_fetch_count +=
        (uint64_t)join_stats_per_partition[i]["outer_disk_fetch"];
  }
  (*join_stats)["inner_disk_fetch"] = inner_disk_fetch_count;
  (*join_stats)["outer_disk_fetch"] = outer_disk_fetch_count;
  return resultBuilder->build();
}

} // namespace li_merge

#endif

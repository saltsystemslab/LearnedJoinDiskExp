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
#include <nlohmann/json.hpp>
#include "inner_inmem_b_tree.h"

using json = nlohmann::json;

namespace li_merge {

template <class T>
SSTable<T> *hash_join(std::unordered_map<std::string, uint64_t> *outer_index,
                      SSTable<T> *inner, SSTableBuilder<T> *result, json *join_stats) {
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
  (*join_stats)["inner_disk_fetch"] = inner_iterator->getDiskFetches();
  (*join_stats)["outer_disk_fetch"] = 0;
  return result->build();
}

template <class T>
SSTable<T> *indexed_nested_loop_join_with_btree(SSTable<T> *outer,
                                     InnerInMemBTree *inner_index,
                                     SSTableBuilder<T> *result,
                                     json *join_stats) {
  auto outer_iterator = outer->iterator();
  while (outer_iterator->valid()) {
    auto kSlice = outer_iterator->key();
#ifdef STRING_KEYS
    std::string key = std::string((kSlice.data()), kSlice.key_size_bytes());
#else
    uint64_t key = *(uint64_t *)(kSlice.data());
#endif
    if (inner_index->keyExists(key)) {
      result->add(outer_iterator->key());
    }
    outer_iterator->next();
  }
  (*join_stats)["inner_disk_fetch"] = inner_index->getDiskFetches(); // inner_iterator->getDiskFetches();
  (*join_stats)["outer_disk_fetch"] = outer_iterator->getDiskFetches();
  return result->build();
}

template <class T>
SSTable<T> *indexed_nested_loop_join(SSTable<T> *outer, SSTable<T> *inner,
                                     Index<T> *inner_index,
                                     Comparator<T> *comparator,
                                     SSTableBuilder<T> *result,
                                     json *join_stats) {
  auto outer_iterator = outer->iterator();
  auto inner_iterator = inner->iterator();
  outer_iterator->seekToFirst();
  inner_iterator->seekToFirst();
  uint64_t inner_num_elts = inner_iterator->numElts();
  while (outer_iterator->valid()) {
    uint64_t approx_pos = inner_index->getPositionBounds(outer_iterator->key()).approx_pos;
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
    while (!is_overshoot && approx_pos < inner_iterator->numElts() &&
           comparator->compare(inner_iterator->peek(approx_pos),
                               outer_iterator->key()) < 0) {
      approx_pos++;
    }

    // No more keys in larger list.
    if (approx_pos == inner_iterator->numElts()) {
      break;
    }
    if (comparator->compare(inner_iterator->peek(approx_pos),
                            outer_iterator->key()) == 0) {
      result->add(outer_iterator->key());
    }
    outer_iterator->next();
  }
  (*join_stats)["inner_disk_fetch"] = inner_iterator->getDiskFetches();
  (*join_stats)["outer_disk_fetch"] = outer_iterator->getDiskFetches();
  return result->build();
}

template <class T>
SSTable<T> *indexed_nested_loop_join_with_pgm(SSTable<T> *outer, SSTable<T> *inner,
                                     Index<T> *inner_index,
                                     Comparator<T> *comparator,
                                     SSTableBuilder<T> *result,
                                     json *join_stats) {
  auto outer_iterator = outer->iterator();
  auto inner_iterator = inner->iterator();
  outer_iterator->seekToFirst();
  inner_iterator->seekToFirst();
  uint64_t inner_num_elts = inner_iterator->numElts();
  while (outer_iterator->valid()) {
    uint64_t approx_pos = inner_index->getPositionBounds(outer_iterator->key()).approx_pos;
    approx_pos = std::min(approx_pos, inner_num_elts - 1);
    // WARNING: A LOT OF HARDCODED ASSUMPTIONS BELOW
    // TODO(chesetti): Generalize these.
    // Rename to load_window, and check.
    // Right now we assume peek loads the correct pages in cache, and then check them.
    // We also assume that the pages loaded are inside the error bound.
    // For 8 + 8 byte keys, and a page size of 4096, a page has 256 keys.
    // So we can handle a max error of 128. We also can't handle 16 byte keys yet.
    inner_iterator->peek(approx_pos);
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
SSTable<T> *presorted_merge_join(SSTable<T> *outer, SSTable<T> *inner,
                                 Comparator<T> *comparator,
                                 SSTableBuilder<T> *result_builder, 
                                 json *join_stats
                                 ) {
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


template<class T>
uint64_t lower_bound(Iterator<T> *iter, Comparator<T> *c, uint64_t lo, uint64_t hi, T key) {
  // Last value that is lesser than or equal to lo
  while (lo < hi) {
    uint64_t mid = lo + (hi-lo+1)/2;
    if (c->compare(iter->peek(mid), key) <= 0) {
      lo = mid;
    } else {
      hi = mid-1;
    }
  }
  return lo;
}



template<class T>
SSTable<T> *presorted_merge_join_exp(SSTable<T> *outer, SSTable<T> *inner,
                                 Comparator<T> *comparator,
                                 SSTableBuilder<T> *result_builder, 
                                 json *join_stats
                                 ) {
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
    comparator->compare(inner_iterator->peek(cur_pos + bound), outer_iterator->key()) < 0) {
      bound = bound * 2;
    }
    uint64_t lim = std::min(inner_nelts-1, cur_pos + bound);
    uint64_t next_pos = lower_bound(inner_iterator, comparator, cur_pos, lim, outer_iterator->key());
    inner_cur_pos = next_pos;
    if (comparator->compare(outer_iterator->key(), inner_iterator->peek(next_pos)) ==
        0) {
      result_builder->add(outer_iterator->key());
    }
    outer_iterator->next();
  }
  (*join_stats)["inner_disk_fetch"] = inner_iterator->getDiskFetches();
  (*join_stats)["outer_disk_fetch"] = outer_iterator->getDiskFetches();
  return result_builder->build();
}

template <class T>
SSTable<T> *parallel_hash_join(int num_threads, SSTable<T> *outer_table,
                               std::unordered_map<std::string, uint64_t> *outer_index,
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
    threads.push_back(
        std::thread(hash_join<T>, outer_index,
                    inner_table->getSSTableForSubRange(inner_start, inner_end),
                    resultBuilder->getBuilderForRange(inner_start + outer_start,
                                                      inner_end + outer_end), &join_stats_per_partition[i]));
  }
  uint64_t inner_disk_fetch_count = 0;
  uint64_t outer_disk_fetch_count = 0;
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
    inner_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["inner_disk_fetch"];
    outer_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["outer_disk_fetch"];
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
        indexed_nested_loop_join<T>, 
                    outer_table->getSSTableForSubRange(outer_start, outer_end),
                    inner_table->getSSTableForSubRange(inner_start, inner_end),
                    inner_index->getIndexForSubrange(inner_start, inner_end), comparator,
                    resultBuilder->getBuilderForRange(inner_start + outer_start,
                                                      inner_end + outer_end), &join_stats_per_partition[i]));
  }
  uint64_t inner_disk_fetch_count = 0;
  uint64_t outer_disk_fetch_count = 0;
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
    inner_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["inner_disk_fetch"];
    outer_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["outer_disk_fetch"];
  }
  (*join_stats)["inner_disk_fetch"] = inner_disk_fetch_count;
  (*join_stats)["outer_disk_fetch"] = outer_disk_fetch_count;
  return resultBuilder->build();
}

template <class T>
SSTable<T> *parallel_indexed_nested_loop_join_with_pgm(
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
        indexed_nested_loop_join_with_pgm<T>, 
                    outer_table->getSSTableForSubRange(outer_start, outer_end),
                    inner_table->getSSTableForSubRange(inner_start, inner_end),
                    inner_index->getIndexForSubrange(inner_start, inner_end), comparator,
                    resultBuilder->getBuilderForRange(inner_start + outer_start,
                                                      inner_end + outer_end), &join_stats_per_partition[i]));
  }
  uint64_t inner_disk_fetch_count = 0;
  uint64_t outer_disk_fetch_count = 0;
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
    inner_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["inner_disk_fetch"];
    outer_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["outer_disk_fetch"];
  }
  (*join_stats)["inner_disk_fetch"] = inner_disk_fetch_count;
  (*join_stats)["outer_disk_fetch"] = outer_disk_fetch_count;
  return resultBuilder->build();
}

template <class T>
SSTable<T> *parallel_presort_join(
    int num_threads, SSTable<T> *outer_table, SSTable<T> *inner_table,
    Index<T> *inner_index, Comparator<T> *comparator,
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
    threads.push_back(
        std::thread(presorted_merge_join<T>, 
                    outer_table->getSSTableForSubRange(outer_start, outer_end),
                    inner_table->getSSTableForSubRange(inner_start, inner_end),
                    comparator,
                    resultBuilder->getBuilderForRange(inner_start + outer_start,
                                                      inner_end + outer_end), &join_stats_per_partition[i]));
  }
  uint64_t inner_disk_fetch_count = 0;
  uint64_t outer_disk_fetch_count = 0;
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
    inner_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["inner_disk_fetch"];
    outer_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["outer_disk_fetch"];
  }
  (*join_stats)["inner_disk_fetch"] = inner_disk_fetch_count;
  (*join_stats)["outer_disk_fetch"] = outer_disk_fetch_count;
  return resultBuilder->build();
}

template <class T>
SSTable<T> *parallel_presort_join_exp(
    int num_threads, SSTable<T> *outer_table, SSTable<T> *inner_table,
    Index<T> *inner_index, Comparator<T> *comparator,
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
    threads.push_back(
        std::thread(presorted_merge_join_exp<T>, 
                    outer_table->getSSTableForSubRange(outer_start, outer_end),
                    inner_table->getSSTableForSubRange(inner_start, inner_end),
                    comparator,
                    resultBuilder->getBuilderForRange(inner_start + outer_start,
                                                      inner_end + outer_end), &join_stats_per_partition[i]));
  }
  uint64_t inner_disk_fetch_count = 0;
  uint64_t outer_disk_fetch_count = 0;
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
    inner_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["inner_disk_fetch"];
    outer_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["outer_disk_fetch"];
  }
  (*join_stats)["inner_disk_fetch"] = inner_disk_fetch_count;
  (*join_stats)["outer_disk_fetch"] = outer_disk_fetch_count;
  return resultBuilder->build();
}

template <class T>
SSTable<T> *parallel_indexed_nested_loop_join_with_btree(
    int num_threads, SSTable<T> *outer_table, SSTable<T> *inner_table,
    InnerInMemBTree *inner_btree,
    PSSTableBuilder<T> *resultBuilder, json *join_stats) {
  uint64_t num_elts_in_outer = outer_table->iterator()->numElts();
  uint64_t num_elts_in_inner = inner_table->iterator()->numElts();
  uint64_t outer_block_size = num_elts_in_outer / num_threads;
  uint64_t outer_spill = num_elts_in_outer % num_threads;
  uint64_t inner_block_size = num_elts_in_inner / num_threads;
  uint64_t inner_spill = num_elts_in_inner % num_threads;
  uint64_t outer_start, outer_end, inner_start, inner_end;
  inner_start = 0;
  outer_start = 0;
  (*join_stats)["inner_disk_fetch"] = 0; 
  (*join_stats)["outer_disk_fetch"] = 0;
  std::vector<std::thread> threads;
  std::vector<json> join_stats_per_partition(num_threads);
  InnerInMemBTree *threadBtrees[num_threads];
  for (int i = 0; i < num_threads; i++) {
    threadBtrees[i] = new InnerInMemBTree(inner_btree);
    outer_end = outer_start + outer_block_size;
    inner_end = inner_start + inner_block_size;
    if (outer_spill) {
      outer_end++;
      outer_spill--;
    }
    if (inner_spill) {
      inner_end++;
      inner_spill--;
    }
    threads.push_back(std::thread(
        indexed_nested_loop_join_with_btree<T>, 
                    outer_table->getSSTableForSubRange(outer_start, outer_end),
                    threadBtrees[i],
                    resultBuilder->getBuilderForRange(inner_start + outer_start,
                                                      inner_end + outer_end), &join_stats_per_partition[i]));
    outer_start = outer_end;
    inner_start = inner_end;
  }
  uint64_t inner_disk_fetch_count = 0;
  uint64_t outer_disk_fetch_count = 0;
  for (int i = 0; i < num_threads; i++) {
    threads[i].join();
    inner_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["inner_disk_fetch"];
    outer_disk_fetch_count += (uint64_t)join_stats_per_partition[i]["outer_disk_fetch"];
  }
  (*join_stats)["inner_disk_fetch"] = inner_disk_fetch_count;
  (*join_stats)["outer_disk_fetch"] = outer_disk_fetch_count;
  return resultBuilder->build();
}
} // namespace li_merge

#endif

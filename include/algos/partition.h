#ifndef PARTITION_H
#define PARTITION_H

#include "comparator.h"
#include "index.h"
#include "sstable.h"
#include <algorithm>
#include <stddef.h>
#include <vector>

namespace li_merge {
struct Partition {
  std::pair<uint64_t, uint64_t> outer;
  std::pair<uint64_t, uint64_t> inner;
};

template <class T>
std::vector<Partition>
partition_sstables(int num_partitions, SSTable<T> *outer_table,
                   SSTable<T> *inner_table, Index<T> *inner_index,
                   Comparator<T> *comparator) {

  std::vector<Partition> partitions;
  Iterator<T> *inner_iter = inner_table->iterator();
  Iterator<T> *outer_iter = outer_table->iterator();
  inner_iter->seekToFirst();
  outer_iter->seekToFirst();

  uint64_t num_items = outer_iter->numElts();
  uint64_t block_size = num_items / num_partitions;
  uint64_t spill = num_items % num_partitions;

  uint64_t outer_start = 0;
  uint64_t outer_end = 0;
  uint64_t inner_start = 0;
  uint64_t inner_end = 0;

  for (int i = 0; i < num_partitions; i++) {
    outer_end = outer_start + block_size;
    if (spill) {
      outer_end++;
      spill--;
    }
    uint64_t inner_end =
        inner_index->getPositionBoundsRA(outer_iter->peek(outer_end - 1)).lower;
    while (inner_end < inner_iter->numElts() &&
           comparator->compare(inner_iter->peek(inner_end),
                               outer_iter->peek(outer_end - 1)) <= 0) {
      inner_end++;
    }
    while (inner_end > 0 &&
           comparator->compare(inner_iter->peek(inner_end - 1),
                               outer_iter->peek(outer_end - 1)) > 0) {
      inner_end--;
    }
    if (i == num_partitions - 1) {
      inner_end = inner_iter->numElts();
    }
    Comparator<T> *thread_comparator = comparator;

    partitions.push_back(
        Partition{std::pair<uint64_t, uint64_t>(outer_start, outer_end),
                  std::pair<uint64_t, uint64_t>(inner_start, inner_end)});
    outer_start = outer_end;
    inner_start = inner_end;
  }
  return partitions;
}
} // namespace li_merge

#endif

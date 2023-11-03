#ifndef LEARNEDINDEXMERGE_MERGE_H
#define LEARNEDINDEXMERGE_MERGE_H

#include "comparator.h"
#include "index.h"
#include "iterator.h"
#include "partition.h"
#include "sstable.h"
#include <algorithm>
#include <nlohmann/json.hpp>
#include <stdint.h>
#include <thread>

using json = nlohmann::json;

namespace li_merge {

template <class T> struct IteratorIndexPair {
  Iterator<T> *iter;
  Index<T> *index;
};

template <class T>
SSTable<T> *standardMerge(SSTable<T> *outer_table, SSTable<T> *inner_table,
                          Comparator<T> *comparator, SSTableBuilder<T> *builder,
                          json *merge_log);

template <class T>
SSTable<T> *parallelStandardMerge(SSTable<T> *outer_table,
                                  SSTable<T> *inner_table,
                                  Index<T> *inner_index,
                                  Comparator<T> *comparator, int num_threads,
                                  PSSTableBuilder<T> *builder, json *merge_log);
template <class T>
SSTable<T> *mergeWithIndexes(SSTable<T> *outer_table, SSTable<T> *inner_table,
                             Index<T> *outer_index, Index<T> *inner_index,
                             Comparator<T> *comparator,
                             SSTableBuilder<T> *resultBuilder, json *merge_log);

template <class T>
SSTable<T> *
mergeWithIndexesThreshold(SSTable<T> *outer_table, SSTable<T> *inner_table,
                          Index<T> *inner_index, uint64_t threshold,
                          Comparator<T> *comparator,
                          SSTableBuilder<T> *resultBuilder, json *merge_log);

namespace internal {
template <class T>
void findTwoSmallest(IteratorIndexPair<T> **iterators, int n,
                     Comparator<T> *comparator, IteratorIndexPair<T> **smallest,
                     IteratorIndexPair<T> **second_smallest);
template <class T>
void findSecondSmallest(IteratorIndexPair<T> **iterators, int n,
                        Comparator<T> *comparator,
                        const IteratorIndexPair<T> *smallest,
                        IteratorIndexPair<T> **second_smallest);
template <class T>
void addClusterToResult(IteratorIndexPair<T> *smallest,
                        IteratorIndexPair<T> *second_smallest,
                        Comparator<T> *comparator,
                        SSTableBuilder<T> *resultBuilder, json *merge_log);
} // namespace internal

} // namespace li_merge

namespace li_merge {

template <class T>
SSTable<T> *standardMerge(SSTable<T> *outer_table, SSTable<T> *inner_table,
                          Comparator<T> *comparator,
                          SSTableBuilder<T> *resultBuilder, json *merge_log) {
#if TRACK_STATS
  comparator = new CountingComparator<T>(comparator);
#endif
  (*merge_log)["inner_disk_fetch"] = 0;
  (*merge_log)["outer_disk_fetch"] = 0;
  Iterator<T> *inner_iter = inner_table->iterator();
  Iterator<T> *outer_iter = outer_table->iterator();
  inner_iter->seekToFirst();
  outer_iter->seekToFirst();

  while (outer_iter->valid()) {
    while (inner_iter->valid() &&
           comparator->compare(inner_iter->key(), outer_iter->key()) <= 0) {
      resultBuilder->add(inner_iter->key());
      inner_iter->next();
    }
    resultBuilder->add(outer_iter->key());
    outer_iter->next();
  }
  while (inner_iter->valid()) {
    resultBuilder->add(inner_iter->key());
    inner_iter->next();
  }
#if TRACK_STATS
  (*merge_log)["comparison_count"] =
      ((CountingComparator<T> *)(comparator))->getCount();
#endif
  (*merge_log)["inner_disk_fetch"] = inner_iter->getDiskFetches();
  (*merge_log)["outer_disk_fetch"] = outer_iter->getDiskFetches();
  return resultBuilder->build();
}

template <class T>
SSTable<T> *mergeWithIndexes(SSTable<T> *outer_table, SSTable<T> *inner_table,
                             Index<T> *outer_index, Index<T> *inner_index,
                             Comparator<T> *comparator,
                             SSTableBuilder<T> *resultBuilder,
                             json *merge_log) {
#if TRACK_STATS
  (*merge_log)["max_index_error_correction"] = 0;
  comparator = new CountingComparator<T>(comparator);
#endif
  (*merge_log)["inner_disk_fetch"] = 0;
  (*merge_log)["outer_disk_fetch"] = 0;

  Iterator<T> *inner_iter = inner_table->iterator();
  Iterator<T> *outer_iter = outer_table->iterator();
  inner_iter->seekToFirst();
  outer_iter->seekToFirst();

  IteratorIndexPair<T> inner = IteratorIndexPair<T>{inner_iter, inner_index};
  IteratorIndexPair<T> outer = IteratorIndexPair<T>{outer_iter, outer_index};
  IteratorIndexPair<T> *iterators[2] = {&inner, &outer};

  IteratorIndexPair<T> *smallest = nullptr;
  IteratorIndexPair<T> *second_smallest = nullptr;
  internal::findTwoSmallest(iterators, 2, comparator, &smallest,
                            &second_smallest);
  while (smallest != nullptr) {
    // There's only one list left, so let's fill up all the way.
    if (second_smallest == nullptr) {
      while (smallest->iter->valid()) {
        resultBuilder->add(smallest->iter->key());
        smallest->iter->next();
      }
      smallest = nullptr;
      continue;
    }
    internal::addClusterToResult(smallest, second_smallest, comparator,
                                 resultBuilder, merge_log);
    smallest = second_smallest;
    second_smallest = nullptr;
    internal::findSecondSmallest(iterators, 2, comparator, smallest,
                                 &second_smallest);
  }
#if TRACK_STATS
  (*merge_log)["comparison_count"] =
      ((CountingComparator<T> *)(comparator))->getCount();
#endif
  (*merge_log)["inner_disk_fetch"] = inner_iter->getDiskFetches();
  (*merge_log)["outer_disk_fetch"] = outer_iter->getDiskFetches();
  return resultBuilder->build();
}

template <class T>
SSTable<T> *
mergeWithIndexesThreshold(SSTable<T> *outer_table, SSTable<T> *inner_table,
                          Index<T> *inner_index, uint64_t threshold,
                          Comparator<T> *comparator,
                          SSTableBuilder<T> *resultBuilder, json *merge_log) {
#if TRACK_STATS
  (*merge_log)["max_index_error_correction"] = 0;
  comparator = new CountingComparator<T>(comparator);
#endif
  (*merge_log)["inner_disk_fetch"] = 0;
  (*merge_log)["outer_disk_fetch"] = 0;

  Iterator<T> *inner_iter = inner_table->iterator();
  Iterator<T> *outer_iter = outer_table->iterator();
  inner_iter->seekToFirst();
  outer_iter->seekToFirst();

  IteratorIndexPair<T> inner = IteratorIndexPair<T>{inner_iter, inner_index};

  while (outer_iter->valid()) {
    if (!inner_iter->valid()) {
      resultBuilder->add(outer_iter->key());
      outer_iter->next();
      continue;
    }
    uint64_t lower_bound =
        inner_index->getPositionBounds(outer_iter->key()).lower;
    lower_bound = std::max(lower_bound, inner_iter->currentPos());
    uint64_t distance = lower_bound - inner_iter->currentPos();
    // We also can do a memcmp here.
    if (distance >= threshold) {
      // Don't copy the bounds.lower item, it could be equal to the item we
      // searched for.
      while (inner_iter->valid() && inner_iter->currentPos() < lower_bound) {
#if DEBUG
        if (comparator->compare(inner_iter->key(), outer_iter->key()) > 0) {
          abort();
        }
#endif
        resultBuilder->add(inner_iter->key());
        inner_iter->next();
      }
    }
    // This can also be made faster, we have already loaded items here into the peek
    // cache. That is an extra IO that can be saved.
    while (inner_iter->valid() &&
           comparator->compare(inner_iter->key(), outer_iter->key()) <= 0) {
      resultBuilder->add(inner_iter->key());
      inner_iter->next();
    }
    resultBuilder->add(outer_iter->key());
    outer_iter->next();
  }

  while (inner_iter->valid()) {
    resultBuilder->add(inner_iter->key());
    inner_iter->next();
  }

#if TRACK_STATS
  (*merge_log)["comparison_count"] =
      ((CountingComparator<T> *)(comparator))->getCount();
#endif
  (*merge_log)["inner_disk_fetch"] = inner_iter->getDiskFetches();
  (*merge_log)["outer_disk_fetch"] = outer_iter->getDiskFetches();
  return resultBuilder->build();
}

namespace internal {

template <class T>
void findTwoSmallest(IteratorIndexPair<T> **iterators, int n,
                     Comparator<T> *comparator, IteratorIndexPair<T> **smallest,
                     IteratorIndexPair<T> **second_smallest) {
  for (int i = 0; i < n; i++) {
    if (!iterators[i]->iter->valid()) {
      continue;
    }
    if (*smallest == nullptr) {
      *smallest = iterators[i];
      continue;
    }
    if (comparator->compare((*smallest)->iter->key(),
                            iterators[i]->iter->key()) > 0) {
      *second_smallest = *smallest;
      *smallest = iterators[i];
      continue;
    }
    if (*second_smallest == nullptr) {
      *second_smallest = iterators[i];
      continue;
    }
    if (comparator->compare((*second_smallest)->iter->key(),
                            iterators[i]->iter->key()) > 0) {
      *second_smallest = iterators[i];
      continue;
    }
  }
}
template <class T>
void findSecondSmallest(IteratorIndexPair<T> **iterators, int n,
                        Comparator<T> *comparator,
                        const IteratorIndexPair<T> *smallest,
                        IteratorIndexPair<T> **second_smallest) {
  for (int i = 0; i < n; i++) {
    if (!iterators[i]->iter->valid()) {
      continue;
    }
    if (smallest == iterators[i]) {
      continue;
    }
    if (*second_smallest == nullptr) {
      *second_smallest = iterators[i];
      continue;
    }
    if (comparator->compare((*second_smallest)->iter->key(),
                            iterators[i]->iter->key()) > 0) {
      *second_smallest = iterators[i];
      continue;
    }
  }
}
template <class T>
void addClusterToResult(IteratorIndexPair<T> *smallest,
                        IteratorIndexPair<T> *second_smallest,
                        Comparator<T> *comparator,
                        SSTableBuilder<T> *resultBuilder, json *merge_log) {
#if TRACK_STATS
  uint64_t error_correction = 0;
#endif
  uint64_t approx_pos =
      smallest->index->getPositionBounds(second_smallest->iter->key())
          .approx_pos;
  approx_pos = std::max(approx_pos, smallest->iter->currentPos());
  approx_pos = std::min(approx_pos, smallest->iter->numElts() - 1);
  bool is_overshoot = false;
  while (comparator->compare(smallest->iter->peek(approx_pos),
                             second_smallest->iter->key()) > 0) {
    is_overshoot = true;
    approx_pos--;
#if TRACK_STATS
    error_correction++;
#endif
  }
  while (smallest->iter->currentPos() <= approx_pos) {
    resultBuilder->add(smallest->iter->key());
    smallest->iter->next();
  }
  if (!is_overshoot) {
    while (smallest->iter->valid() &&
           comparator->compare(smallest->iter->key(),
                               second_smallest->iter->key()) <= 0) {
      resultBuilder->add(smallest->iter->key());
      smallest->iter->next();
#if TRACK_STATS
      error_correction++;
#endif
    }
  }
#if TRACK_STATS
  (*merge_log)["max_index_error_correction"] = std::max<uint64_t>(
      (uint64_t)(*merge_log)["max_index_error_correction"], error_correction);
#endif
}

} // namespace internal

} // namespace li_merge
#endif // LEARNEDINDEXMERGE_MERGE_H

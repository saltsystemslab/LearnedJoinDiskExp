#ifndef LEARNEDINDEXMERGE_MERGE_H
#define LEARNEDINDEXMERGE_MERGE_H

#include "join.h"
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
class StandardMerge: public BaseMergeAndJoinOp<T> {
  public:
    StandardMerge(
        SSTable<T> *outer, 
        SSTable<T> *inner, 
        IndexBuilder<T> *inner_index_builder,
        Comparator<T> *comparator,
        PSSTableBuilder<T> *result_builder,
        int num_threads):
    BaseMergeAndJoinOp<T>(outer, inner, inner_index_builder, comparator, result_builder, num_threads) {}

    void doOpOnPartition(Partition partition, TableOpResult<T> *result) override {
      uint64_t outer_start = partition.outer.first;
      uint64_t outer_end = partition.outer.second;
      uint64_t inner_start = partition.inner.first;
      uint64_t inner_end = partition.inner.second;

      auto outer_iter = this->outer_->iterator();
      auto inner_iter = this->inner_->iterator();
      auto result_builder = this->result_builder_->getBuilderForRange(inner_start + outer_start, inner_end + outer_end);

      outer_iter->seekTo(outer_start);
      inner_iter->seekTo(inner_start);

      while (outer_iter->currentPos() < outer_end) {
        while (inner_iter->currentPos() < inner_end &&
               this->comparator_->compare(inner_iter->key(), outer_iter->key()) <= 0) {
          result_builder->add(inner_iter->key());
          inner_iter->next();
        }
        result_builder->add(outer_iter->key());
        outer_iter->next();
      }
      while (inner_iter->valid()) {
        result_builder->add(inner_iter->key());
        inner_iter->next();
      }
      result->stats["inner_disk_fetch"] = inner_iter->getDiskFetches();
      result->stats["outer_disk_fetch"] = outer_iter->getDiskFetches();
      result->output_table = result_builder->build(),
      delete outer_iter;
      delete inner_iter;
    }
};

template <class T>
class LearnedMerge1Way: public BaseMergeAndJoinOp<T> {
  public:
    LearnedMerge1Way(
        SSTable<T> *outer, 
        SSTable<T> *inner, 
        IndexBuilder<T> *inner_index_builder,
        Comparator<T> *comparator,
        SearchStrategy<T> *search_strategy,
        PSSTableBuilder<T> *result_builder,
        int num_threads):
    BaseMergeAndJoinOp<T>(outer, inner, inner_index_builder, comparator, result_builder, num_threads),
    search_strategy_(search_strategy) {}

    void doOpOnPartition(Partition partition, TableOpResult<T> *result) override {
      uint64_t outer_start = partition.outer.first;
      uint64_t outer_end = partition.outer.second;
      uint64_t inner_start = partition.inner.first;
      uint64_t inner_end = partition.inner.second;

      auto outer_iter = this->outer_->iterator();
      auto inner_iter = this->inner_->windowIterator();
      auto inner_index = this->inner_index_; // TODO Make a Copy.
      auto result_builder = this->result_builder_->getBuilderForRange(inner_start + outer_start, inner_end + outer_end);
      uint64_t next_inner_key_to_add = inner_start;

      while (outer_iter->currentPos() < outer_end) {
        auto bounds =
          inner_index->getPositionBounds(outer_iter->key());
        // Don't go back and search in a page you already looked at for a smaller key.
        bounds.lower = std::max(bounds.lower, next_inner_key_to_add);
        bounds.upper = std::min(bounds.upper, inner_end);

        // The next key is greater in inner, so just copy the outer key and get out.
        if (bounds.upper <= bounds.lower) {
          result_builder->add(outer_iter->key());
          outer_iter->next();
          continue;
        }
        
        // Blind Copy all keys before lower_bound
        if (bounds.lower > next_inner_key_to_add) {
          auto window = inner_iter->getWindow(next_inner_key_to_add, bounds.lower);
          while (window.hi_idx < bounds.lower) {
            result_builder->addWindow(window);
            next_inner_key_to_add = window.hi_idx;
            window = inner_iter->getWindow(next_inner_key_to_add, bounds.lower);
          }
          result_builder->addWindow(window);
        }

        SearchResult result;
        do {
          auto window = inner_iter->getWindow(bounds.lower, bounds.upper);
          result = search_strategy_->search(window, outer_iter->key(), bounds);
          // Copy all keys from [window.lo_idx, result.bounds.lower)
          window.hi_idx = result.lower_bound;
          result_builder->addWindow(window);
          
          next_inner_key_to_add = window.hi_idx;
          bounds.lower = window.hi_idx; // If you search next time, search from here.
        } while (result.shouldContinue);

        next_inner_key_to_add = result.lower_bound; // Never search before this again.
        result_builder->add(outer_iter->key());
        outer_iter->next();
      }

      while (next_inner_key_to_add < inner_end) {
        auto window = inner_iter->getWindow(next_inner_key_to_add, inner_end);
        result_builder->addWindow(window);
        next_inner_key_to_add = window.hi_idx;
      }

      result->stats["inner_disk_fetch"] = inner_iter->getDiskFetches();
      result->stats["outer_disk_fetch"] = outer_iter->getDiskFetches();
      result->output_table = result_builder->build(),
      delete outer_iter;
      delete inner_iter;
    }
  private:
    SearchStrategy<T> *search_strategy_;
};

template <class T>
SSTable<T> *mergeWithIndexes(SSTable<T> *outer_table, SSTable<T> *inner_table,
                             Index<T> *outer_index, Index<T> *inner_index,
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

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
        Index<T> *inner_index,
        Comparator<T> *comparator,
        PSSTableBuilder<T> *result_builder,
        int num_threads):
    BaseMergeAndJoinOp<T>(outer, inner, inner_index, comparator, result_builder, num_threads) {}

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
      while (inner_iter->currentPos() < inner_end) {
        result_builder->add(inner_iter->key());
        inner_iter->next();
      }
      result->stats["inner_disk_fetch"] = inner_iter->getDiskFetches();
      result->stats["inner_disk_fetch_size"] = inner_iter->getDiskFetchSize();
      result->stats["inner_total_bytes_fetched"] = inner_iter->getTotalBytesFetched();
      result->stats["outer_disk_fetch"] = outer_iter->getDiskFetches();
      result->stats["outer_disk_fetch_size"] = outer_iter->getDiskFetchSize();
      result->stats["outer_total_bytes_fetched"] = outer_iter->getTotalBytesFetched();

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
        Index<T> *inner_index,
        Comparator<T> *comparator,
        SearchStrategy<T> *search_strategy,
        PSSTableBuilder<T> *result_builder,
        int num_threads):
    BaseMergeAndJoinOp<T>(outer, inner, inner_index, comparator, result_builder, num_threads),
    search_strategy_(search_strategy) {}

    void doOpOnPartition(Partition partition, TableOpResult<T> *result) override {
      uint64_t outer_start = partition.outer.first;
      uint64_t outer_end = partition.outer.second;
      uint64_t inner_start = partition.inner.first;
      uint64_t inner_end = partition.inner.second;

      auto outer_iter = this->outer_->iterator();
      auto inner_iter = this->inner_->windowIterator();
      auto inner_index = this->inner_index_->shallow_copy(); // TODO: Add a free_shallow_copy method.
      auto result_builder = this->result_builder_->getBuilderForRange(inner_start + outer_start, inner_end + outer_end);
      uint64_t next_inner_key_to_add = inner_start;

      outer_iter->seekTo(outer_start);
      // inner_iter->setWindowSize(inner_index->getMaxError());

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
      result->stats["inner_disk_fetch_size"] = inner_iter->getDiskFetchSize();
      result->stats["inner_total_bytes_fetched"] = inner_iter->getTotalBytesFetched();
      result->stats["outer_disk_fetch"] = outer_iter->getDiskFetches();
      result->stats["outer_disk_fetch_size"] = outer_iter->getDiskFetchSize();
      result->stats["outer_total_bytes_fetched"] = outer_iter->getTotalBytesFetched();

      result->output_table = result_builder->build(),
      delete outer_iter;
      delete inner_iter;
    }
  private:
    SearchStrategy<T> *search_strategy_;
};

} // namespace li_merge
#endif // LEARNEDINDEXMERGE_MERGE_H

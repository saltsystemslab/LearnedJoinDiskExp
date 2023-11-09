#ifndef LEARNEDINDEXMERGE_JOIN_H
#define LEARNEDINDEXMERGE_JOIN_H

#include "comparator.h"
#include "index.h"
#include "iterator.h"
#include "key_value_slice.h"
#include "partition.h"
#include "sstable.h"
#include "search.h"
#include <thread>
#include <unordered_set>
// TODO(chesetti): Must be last for some reason. Fix.
#include <nlohmann/json.hpp>
#include "table_op.h"

using json = nlohmann::json;

namespace li_merge {

template <class T>
class BaseMergeAndJoinOp: public TableOp<T> {
public:
    BaseMergeAndJoinOp(
        SSTable<T> *outer, 
        SSTable<T> *inner, 
        IndexBuilder<T> *inner_index_builder,
        Comparator<T> *comparator,
        PSSTableBuilder<T> *result_builder,
        int num_threads):
     TableOp<T>(outer, inner, result_builder, num_threads),
     comparator_(comparator),
     inner_index_builder_(inner_index_builder) {}

  void preOp() override {
      // Build Inner Index. You will always need this to partition.
      Iterator<T> *inner_iter = this->inner_->iterator();
      auto inner_index_build_begin = std::chrono::high_resolution_clock::now();
      while (inner_iter->valid()) {
        inner_index_builder_->add(inner_iter->key());
        inner_iter->next();
      }
      inner_index_ = this->inner_index_builder_->build();
      auto inner_index_build_end = std::chrono::high_resolution_clock::now();
      auto inner_index_build_duration_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            inner_index_build_end - inner_index_build_begin).count();
      this->stats_["inner_index_build_duration_ns"] = inner_index_build_duration_ns;
      this->stats_["inner_index_size"] = inner_index_->sizeInBytes();
      delete inner_iter;
  }

  std::vector<Partition> getPartitions() override {
    return partition_sstables(this->num_threads_, this->outer_, this->inner_,
                                      inner_index_, comparator_);
  }

  void mergePartitions() override {
    uint64_t inner_disk_fetch_count = 0;
    uint64_t outer_disk_fetch_count = 0;
    for (int i = 0; i < this->num_threads_; i++) {
        inner_disk_fetch_count +=
          (uint64_t)(this->partition_results_[i].stats["inner_disk_fetch"]);
        outer_disk_fetch_count +=
          (uint64_t)(this->partition_results_[i].stats["outer_disk_fetch"]);
      }
    this->stats_["inner_disk_fetch"] = inner_disk_fetch_count;
    this->stats_["outer_disk_fetch"] = outer_disk_fetch_count;
    this->output_table_ = this->result_builder_->build();
  }
protected:
    IndexBuilder<T> *inner_index_builder_;
    Index<T> *inner_index_;
    Comparator<T> *comparator_;
};

template <class T>
class LearnedIndexInlj: public BaseMergeAndJoinOp<T> {
  public:
    LearnedIndexInlj(
        SSTable<T> *outer, 
        SSTable<T> *inner, 
        IndexBuilder<T> *inner_index_builder,
        Comparator<T> *comparator,
        SearchStrategy<T> *search_strategy,
        PSSTableBuilder<T> *result_builder,
        int num_threads):
    BaseMergeAndJoinOp<T>(outer, inner, inner_index_builder, comparator, result_builder, num_threads),
    search_strategy_(search_strategy), last_found_idx_(0) {}

    void doOpOnPartition(Partition partition, TableOpResult<T> *result) override {
      uint64_t outer_start = partition.outer.first;
      uint64_t outer_end = partition.outer.second;
      uint64_t inner_start = partition.inner.first;
      uint64_t inner_end = partition.inner.second;

      auto outer_iterator = this->outer_->iterator();
      auto inner_iterator = this->inner_->windowIterator();
      auto inner_index = this->inner_index_; // TODO Make a Copy.
      auto result_builder = this->result_builder_->getBuilderForRange(inner_start + outer_start, inner_end + outer_end);

      inner_iterator->setWindowSize(inner_index->getMaxError());
      outer_iterator->seekTo(outer_start);

      while (outer_iterator->currentPos() < outer_end) {
        auto bounds =
          inner_index->getPositionBounds(outer_iterator->key());
        // Don't go back and search in a page you already looked at for a smaller key.
        bounds.lower = std::max(bounds.lower, last_found_idx_);
        bounds.upper = std::min(inner_end, bounds.upper);
        if (bounds.upper <= bounds.lower) {
          outer_iterator->next();
          continue;
        }
        SearchResult result;
#if DEBUG
        assert(bounds.upper > bounds.lower);
        LinearSearch expected_search;
        SearchResult expected_result;
#endif
        do {
          auto window = inner_iterator->getWindow(bounds.lower, bounds.upper);
          result = search_strategy_->search(window, outer_iterator->key(), bounds);
#if DEBUG
          expected_result = expected_search.search(window, outer_iterator->key(), bounds);
          if (result.found != expected_result.found || 
              result.lower_bound != expected_result.lower_bound ||
              result.shouldContinue != expected_result.shouldContinue) {
            result = search_strategy_->search(window, outer_iterator->key(), bounds);
            expected_result = expected_search.search(window, outer_iterator->key(), bounds);
            abort();
          }
#endif
          bounds.lower = window.hi_idx; // If you search next time, search from here.
        } while (result.shouldContinue);
        if (result.found) {
          result_builder->add(outer_iterator->key());
        }
        last_found_idx_ = result.lower_bound; // Never search before this again.
        outer_iterator->next();
      }
      result->stats["inner_disk_fetch"] = inner_iterator->getDiskFetches();
      result->stats["outer_disk_fetch"] = outer_iterator->getDiskFetches();
      result->output_table = result_builder->build(),
      delete outer_iterator;
      delete inner_iterator;
    }
  private:
    uint64_t last_found_idx_;
    SearchStrategy<T> *search_strategy_;
};

// TODO: Generalize to all types.
class HashJoin: public BaseMergeAndJoinOp<KVSlice> {
  public:
    HashJoin(
        SSTable<KVSlice> *outer, 
        SSTable<KVSlice> *inner, 
        IndexBuilder<KVSlice> *inner_index_builder,
        Comparator<KVSlice> *comparator,
        PSSTableBuilder<KVSlice> *result_builder,
        int num_threads):
    BaseMergeAndJoinOp<KVSlice>(outer, inner, inner_index_builder, comparator, result_builder, num_threads) {}

  void preOp() override {
    BaseMergeAndJoinOp<KVSlice>::preOp();
    Iterator<KVSlice> *outer_iter = this->outer_->iterator();
    while (outer_iter->valid()) {
      KVSlice kv = outer_iter->key();
      std::string key(kv.data(), kv.key_size_bytes());
      if (outer_hash_map.find(key) == outer_hash_map.end()) {
        outer_hash_map[key] = 0;
      }
      outer_hash_map[key]++;
      outer_iter->next();
    }
  }

  void doOpOnPartition(Partition partition, TableOpResult<KVSlice> *result) override {
      uint64_t outer_start = partition.outer.first;
      uint64_t outer_end = partition.outer.second;
      uint64_t inner_start = partition.inner.first;
      uint64_t inner_end = partition.inner.second;

      auto inner_iterator = this->inner_->iterator();
      auto outer_iterator = this->outer_->iterator();
      auto result_builder = this->result_builder_->getBuilderForRange(inner_start + outer_start, inner_end + outer_end);

      inner_iterator->seekTo(inner_start);

       std::string prev;
       while (inner_iterator->currentPos() < inner_end) {
         KVSlice kv = inner_iterator->key();
         std::string const key(kv.data(), kv.key_size_bytes());
         if (outer_hash_map.find(key) != outer_hash_map.end() && prev != key) {
           int repeats = outer_hash_map.at(key);
           for (int i = 0; i < repeats; i++)
             result_builder->add(inner_iterator->key());
         }
         prev = key;
         inner_iterator->next();
       }
      result->stats["inner_disk_fetch"] = inner_iterator->getDiskFetches();
      result->stats["outer_disk_fetch"] = outer_iterator->getDiskFetches();
      result->output_table = result_builder->build();
  }

  private:
    std::unordered_map<std::string, uint64_t> outer_hash_map;
};

template <class T>
class SortJoin: public BaseMergeAndJoinOp<T> {
  public:
    SortJoin(
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

      auto outer_iterator = this->outer_->iterator();
      auto inner_iterator = this->inner_->iterator();
      auto result_builder = this->result_builder_->getBuilderForRange(inner_start + outer_start, inner_end + outer_end);
      outer_iterator->seekTo(outer_start);
      inner_iterator->seekTo(inner_start);

       while (outer_iterator->currentPos() < outer_end) {
         while (inner_iterator->currentPos() < inner_end &&
                (this->comparator_->compare(inner_iterator->key(), outer_iterator->key()) <
                 0)) {
           inner_iterator->next();
         }
         if (!(inner_iterator->currentPos() < inner_end)) {
           break;
         }
         if (this->comparator_->compare(outer_iterator->key(), inner_iterator->key()) ==
             0) {
           result_builder->add(inner_iterator->key());
         }
         outer_iterator->next();
       }

      result->stats["inner_disk_fetch"] = inner_iterator->getDiskFetches();
      result->stats["outer_disk_fetch"] = outer_iterator->getDiskFetches();
      result->output_table = result_builder->build(),

      delete outer_iterator;
      delete inner_iterator;
    }
};


template <class T>
class SortJoinExpSearch: public BaseMergeAndJoinOp<T> {
  private:
    uint64_t lower_bound(Iterator<T> *iter, uint64_t lo, uint64_t hi, T key) {
    // Last value that is lesser than or equal to lo
    while (lo < hi) {
      uint64_t mid = lo + (hi - lo + 1) / 2;
      if (this->comparator_->compare(iter->peek(mid), key) <= 0) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }
    return lo;
  }
  public:
    SortJoinExpSearch(
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

      auto outer_iterator = this->outer_->iterator();
      auto inner_iterator = this->inner_->iterator();
      auto result_builder = this->result_builder_->getBuilderForRange(inner_start + outer_start, inner_end + outer_end);
      outer_iterator->seekTo(outer_start);
      inner_iterator->seekTo(inner_start);

      uint64_t inner_cur_pos = inner_start;
      while (outer_iterator->currentPos() < outer_end) {
        uint64_t cur_pos = inner_cur_pos;
        uint64_t bound = 1;
        while (bound + cur_pos < inner_end &&
           this->comparator_->compare(inner_iterator->peek(cur_pos + bound),
                               outer_iterator->key()) < 0) {
          bound = bound * 2;
        }
        uint64_t lim = std::min(inner_end - 1, cur_pos + bound);
        uint64_t next_pos = lower_bound(inner_iterator, cur_pos, lim, outer_iterator->key());
        inner_cur_pos = next_pos;
        if (this->comparator_->compare(outer_iterator->key(),
                            inner_iterator->peek(next_pos)) == 0) {
          result_builder->add(outer_iterator->key());
        }
        outer_iterator->next();
      }

      result->stats["inner_disk_fetch"] = inner_iterator->getDiskFetches();
      result->stats["outer_disk_fetch"] = outer_iterator->getDiskFetches();
      result->output_table = result_builder->build(),

      delete outer_iterator;
      delete inner_iterator;
    }
};

} // namespace li_merge

#endif

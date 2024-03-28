#ifndef LEARNEDINDEXMERGE_JOIN_H
#define LEARNEDINDEXMERGE_JOIN_H

#include "comparator.h"
#include "index.h"
#include "iterator.h"
#include "key_value_slice.h"
#include "partition.h"
#include "search.h"
#include "sstable.h"
#include <thread>
#include <unordered_set>
// TODO(chesetti): Must be last for some reason. Fix.
#include <nlohmann/json.hpp>
#include "table_op.h"
#include "b_tree.h"
#include "pgm/pgm_index.hpp"
#include "pgm/pgm_index_variants.hpp"

using json = nlohmann::json;

namespace li_merge {

template <class T> class BaseMergeAndJoinOp : public TableOp<T> {
public:
  BaseMergeAndJoinOp(SSTable<T> *outer, SSTable<T> *inner,
                     Index<T> *inner_index, Comparator<T> *comparator,
                     PSSTableBuilder<T> *result_builder, int num_threads)
      : TableOp<T>(outer, inner, result_builder, num_threads),
        comparator_(comparator), inner_index_(inner_index) {}

  std::vector<Partition> getPartitions() override {
    return partition_sstables(this->num_threads_, this->outer_, this->inner_,
                              inner_index_, comparator_);
  }

  void mergePartitions() override {
    uint64_t inner_disk_fetch_count = 0;
    uint64_t inner_disk_fetch_size = 0;
    uint64_t inner_total_bytes_fetched = 0;
    uint64_t outer_disk_fetch_count = 0;
    uint64_t outer_total_bytes_fetched = 0;
    uint64_t outer_disk_fetch_size = 0;
    for (int i = 0; i < this->num_threads_; i++) {
      inner_disk_fetch_count +=
          (uint64_t)(this->partition_results_[i].stats["inner_disk_fetch"]);
      inner_disk_fetch_size = (uint64_t)(
          this->partition_results_[i].stats["inner_disk_fetch_size"]);
      inner_total_bytes_fetched += (uint64_t)(
          this->partition_results_[i].stats["inner_total_bytes_fetched"]);
      outer_disk_fetch_count +=
          (uint64_t)(this->partition_results_[i].stats["outer_disk_fetch"]);
      outer_disk_fetch_size = (uint64_t)(
          this->partition_results_[i].stats["outer_disk_fetch_size"]);
      outer_total_bytes_fetched += (uint64_t)(
          this->partition_results_[i].stats["outer_total_bytes_fetched"]);
    }
    this->stats_["inner_disk_fetch"] = inner_disk_fetch_count;
    this->stats_["inner_disk_fetch_size"] = inner_disk_fetch_size;
    this->stats_["inner_total_bytes_fetched"] = inner_total_bytes_fetched;
    this->stats_["outer_disk_fetch"] = outer_disk_fetch_count;
    this->stats_["outer_disk_fetch_size"] = outer_disk_fetch_size;
    this->stats_["outer_total_bytes_fetched"] = outer_total_bytes_fetched;
    this->output_table_ = this->result_builder_->build();
  }

protected:
  Index<T> *inner_index_;
  Comparator<T> *comparator_;
};

template <class T> class LearnedSortJoinOnUnsortedData: public TableOp<T> {
  std::string index_file_;
  public:
  LearnedSortJoinOnUnsortedData(SSTable<T> *outer, SSTable<T> *inner,
                  PSSTableBuilder<T> *result_builder, int num_threads, std::string index_file)
      : TableOp<T>(outer, inner, result_builder, num_threads), index_file_(index_file) {}
  std::vector<Partition> getPartitions() override {
    if (this->num_threads_ != 1) {
      abort();
    }
    uint64_t outer_start = 0;
    uint64_t outer_end = this->outer_->iterator()->numElts();
    uint64_t inner_start = 0;
    uint64_t inner_end = this->inner_->iterator()->numElts();
    std::vector<Partition> partitions;
    partitions.push_back(
        Partition{std::pair<uint64_t, uint64_t>(outer_start, outer_end),
                  std::pair<uint64_t, uint64_t>(inner_start, inner_end)});
    return partitions;
  }

  void doOpOnPartition(Partition partition, TableOpResult<T> *result) override {
    uint64_t outer_start = partition.outer.first;
    uint64_t outer_end = partition.outer.second;
    uint64_t inner_start = partition.inner.first;
    uint64_t inner_end = partition.inner.second;
    auto result_builder = this->result_builder_->getBuilderForRange(
        inner_start + outer_start, inner_end + outer_end);

    // Build the CDF.
    std::vector<uint64_t> sampled_keys;
    uint64_t num_keys = outer_end - outer_start;
    uint64_t sample_freq = 100;
    uint64_t sample_step = num_keys / sample_freq;
    auto outer_iter = this->outer_->iterator();
    auto inner_iter = this->inner_->iterator();
    for (uint64_t i=outer_start; i < outer_end; i+=sample_freq) {
      outer_iter->seekTo(i);
      KVSlice k = outer_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      sampled_keys.push_back(key);
    }
    sort(sampled_keys.begin(), sampled_keys.end());
    auto index = new pgm::PGMIndex<uint64_t, 1>(sampled_keys);

    // Calculate buckets 
    uint64_t last_pos = 0;
    uint64_t num_buckets = num_keys / (256) + 64;
    uint64_t max_key = 0;
    uint64_t *bucket_sizes = new uint64_t[num_buckets + 64];
    uint64_t *bucket_prefix = new uint64_t[num_buckets + 64];
    uint64_t *bucket_max = new uint64_t[num_buckets + 64];
    uint64_t *bucket_cur = new uint64_t[num_buckets + 64];
    memset((void *)bucket_sizes, 0, sizeof(uint64_t) * (num_buckets + 64));
    memset((void *)bucket_prefix, 0, sizeof(uint64_t) * (num_buckets + 64));
    memset((void *)bucket_max, 0, sizeof(uint64_t) * (num_buckets + 64));

    // Write the buckets 
    outer_iter->seekTo(0);
    for (uint64_t i=outer_start; i < outer_end; i++) {
      KVSlice k = outer_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      max_key = std::max(max_key, key);
      auto pos = index->search(key).pos;
      // pos is scaled to [0, num_keys/100]
      uint64_t bucket = (pos * 1.0 * sample_freq / num_keys) * num_buckets;
      if (bucket >= num_buckets + 64) {
        abort();
      }
      bucket_sizes[bucket]++;
      bucket_max[bucket] = std::max(bucket_max[bucket], key);
      // result_builder->add(k);
      outer_iter->next();
    }
    uint64_t max_buffer_size = bucket_sizes[0];
    for (uint64_t i=1; i<num_buckets + 64; i++)  {
      bucket_prefix[i] = bucket_prefix[i-1] + bucket_sizes[i-1];
      bucket_max[i] = std::max(bucket_max[i], bucket_max[i-1]);
      max_buffer_size = std::max(bucket_sizes[i], max_buffer_size);
    }
    int fd = open(index_file_.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd == -1) {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }
    outer_iter->seekTo(0);
    for (uint64_t i=outer_start; i<outer_end; i++) {
      KVSlice k = outer_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      auto pos = index->search(key).pos;
      // pos is scaled to [0, num_keys/100]
      uint64_t b= (pos * 1.0 * sample_freq / num_keys) * num_buckets;
      if (b >= num_buckets + 64) {
        abort();
      }
      uint64_t offset = (bucket_prefix[b] + bucket_cur[b]) * sizeof(uint64_t);
      pwrite(fd, k.data(), sizeof(uint64_t), offset);
      bucket_cur[b]++;
      outer_iter->next();
    }
    close(fd);

    // Stream the inner and check if inside.
    uint64_t *buffer = new uint64_t[max_buffer_size];
    fd = open(index_file_.c_str(), O_RDONLY, 0644);
    inner_iter->seekTo(0);
    for (uint64_t i=inner_start; i<inner_end; i++) {
      KVSlice k = inner_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      if (key > max_key) {
        inner_iter->next();
        continue;
      }
      if (key == max_key) {
        inner_iter->next();
        result_builder->add(k);
        continue;
      }
      auto pos = index->search(key).pos;
      // pos is scaled to [0, num_keys/100]
      uint64_t b= (pos * 1.0 * sample_freq / num_keys) * num_buckets;
      if (b >= num_buckets + 64) {
        b = num_buckets + 63;
      }
      bool exceeds_max = false;
      if (key > bucket_max[b]) {
        exceeds_max = true;
        inner_iter->next();
        continue;
      }
      pread(fd, buffer, bucket_sizes[b] * sizeof(uint64_t), bucket_prefix[b] * sizeof(uint64_t));
      bool found = false;
      for (uint64_t j=0; j<bucket_sizes[b]; j++) {
        if (buffer[j] == key) {
          found = true;
          result_builder->add(k);
        }
      }
      inner_iter->next();
    }
    close(fd);
    result->output_table = result_builder->build();
  }

  void mergePartitions() override {
    this->output_table_ = this->result_builder_->build();
  }
};

template <class T> class IndexedJoinOnUnsortedData: public TableOp<T> {
  public:
  std::string btree_file_path_;
  void postOp() override {
    // std::remove(btree_file_path_.c_str());
  }
  IndexedJoinOnUnsortedData(SSTable<T> *outer, SSTable<T> *inner,
                  PSSTableBuilder<T> *result_builder, int num_threads, std::string btree_file_path)
      : TableOp<T>(outer, inner, result_builder, num_threads), btree_file_path_(btree_file_path) {}

  std::vector<Partition> getPartitions() override {
    if (this->num_threads_ != 1) {
      abort();
    }
    uint64_t outer_start = 0;
    uint64_t outer_end = this->outer_->iterator()->numElts();
    uint64_t inner_start = 0;
    uint64_t inner_end = this->inner_->iterator()->numElts();
    std::vector<Partition> partitions;
    partitions.push_back(
        Partition{std::pair<uint64_t, uint64_t>(outer_start, outer_end),
                  std::pair<uint64_t, uint64_t>(inner_start, inner_end)});
    return partitions;
  }

  void doOpOnPartition(Partition partition, TableOpResult<T> *result) override {
    uint64_t outer_start = partition.outer.first;
    uint64_t outer_end = partition.outer.second;
    uint64_t inner_start = partition.inner.first;
    uint64_t inner_end = partition.inner.second;
    auto result_builder = this->result_builder_->getBuilderForRange(
        inner_start + outer_start, inner_end + outer_end);

    LeafNodeIterm lni;
    int c;
    uint64_t value;
    char *btreeName = new char[btree_file_path_.size() + 1];
    memset(btreeName, '\0', btree_file_path_.size() + 1);
    memcpy(btreeName, btree_file_path_.c_str(), btree_file_path_.size());
    BTree *btree = new BTree(LEAF_DISK, true, btreeName);
    delete btreeName;

    auto outer_iter = this->outer_->iterator();
    for (uint64_t i=outer_start; i<outer_end; i++) {
      KVSlice k = outer_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      lni.key = key; lni.value = i;
      btree->insert_key_leaf_disk(lni);
      outer_iter->next();
    }

    uint64_t pos;
    auto inner_iter = this->inner_->iterator();
    for (uint64_t i=inner_start; i<inner_end; i++) {
      KVSlice k = inner_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      if (btree->lookup_value(key, &pos, &c)) {
        // TODO: Read pos and lookup on disk for value.
        // TODO: Sort output.
        result_builder->add(k);
      }
      inner_iter->next();
    }
    delete btree;
    result->output_table = result_builder->build();
  }

  void mergePartitions() override {
    this->output_table_ = this->result_builder_->build();
  }

};

template <class T> class LearnedSortJoin : public BaseMergeAndJoinOp<T> {
public:
  LearnedSortJoin(SSTable<T> *outer, SSTable<T> *inner, Index<T> *inner_index,
                  Comparator<T> *comparator, SearchStrategy<T> *search_strategy,
                  PSSTableBuilder<T> *result_builder, int num_threads)
      : BaseMergeAndJoinOp<T>(outer, inner, inner_index, comparator,
                              result_builder, num_threads),
        search_strategy_(search_strategy) {}

  void doOpOnPartition(Partition partition, TableOpResult<T> *result) override {
    uint64_t outer_start = partition.outer.first;
    uint64_t outer_end = partition.outer.second;
    uint64_t inner_start = partition.inner.first;
    uint64_t inner_end = partition.inner.second;

    auto outer_iterator = this->outer_->iterator();
    auto inner_iterator = this->inner_->windowIterator();
    auto inner_index =
        this->inner_index_
            ->shallow_copy(); // TODO: Add a free_shallow_copy method.
    auto result_builder = this->result_builder_->getBuilderForRange(
        inner_start + outer_start, inner_end + outer_end);
    uint64_t last_found_idx = 0;

    inner_iterator->setWindowSize(inner_index->getMaxError());
    outer_iterator->seekTo(outer_start);

    while (outer_iterator->currentPos() < outer_end) {
      auto bounds = inner_index->getPositionBounds(outer_iterator->key());
      // Don't go back and search in a page you already looked at for a smaller
      // key.
      bounds.lower = std::max(bounds.lower, last_found_idx);
      bounds.upper = std::min(inner_end, bounds.upper);
      if (bounds.upper <= bounds.lower) {
        outer_iterator->next();
        continue;
      }
      SearchResult result;
      do {
        auto window = inner_iterator->getWindow(bounds.lower, bounds.upper);
        result = search_strategy_->search(window, outer_iterator->key(), bounds,
                                          this->comparator_);
        bounds.lower =
            window.hi_idx; // If you search next time, search from here.
        if (bounds.lower == inner_end)
          break; // If no more items to read, break.
      } while (result.shouldContinue);
      if (result.found) {
        result_builder->add(outer_iterator->key());
      } 
      last_found_idx = result.lower_bound; // Never search before this again.
      outer_iterator->next();
    }
    result->stats["inner_disk_fetch"] = inner_iterator->getDiskFetches();
    result->stats["inner_disk_fetch_size"] = inner_iterator->getDiskFetchSize();
    result->stats["inner_total_bytes_fetched"] =
        inner_iterator->getTotalBytesFetched();
    result->stats["outer_disk_fetch"] = outer_iterator->getDiskFetches();
    result->stats["outer_disk_fetch_size"] = outer_iterator->getDiskFetchSize();
    result->stats["outer_total_bytes_fetched"] =
        outer_iterator->getTotalBytesFetched();
    result->output_table = result_builder->build(), delete outer_iterator;
    delete inner_iterator;
  }

private:
  SearchStrategy<T> *search_strategy_;
};

template <class T> class Inlj : public BaseMergeAndJoinOp<T> {
public:
  Inlj(SSTable<T> *outer, SSTable<T> *inner, Index<T> *inner_index,
       Comparator<T> *comparator, SearchStrategy<T> *search_strategy,
       PSSTableBuilder<T> *result_builder, int num_threads)
      : BaseMergeAndJoinOp<T>(outer, inner, inner_index, comparator,
                              result_builder, num_threads),
        search_strategy_(search_strategy) {}

  void doOpOnPartition(Partition partition, TableOpResult<T> *result) override {
    uint64_t outer_start = partition.outer.first;
    uint64_t outer_end = partition.outer.second;
    uint64_t inner_start = partition.inner.first;
    uint64_t inner_end = partition.inner.second;

    auto outer_iterator = this->outer_->iterator();
    auto inner_iterator = this->inner_->windowIterator();
    auto inner_index =
        this->inner_index_
            ->shallow_copy(); // TODO: Add a free_shallow_copy method.
    auto result_builder = this->result_builder_->getBuilderForRange(
        inner_start + outer_start, inner_end + outer_end);
    uint64_t last_found_idx = 0;

    inner_iterator->setWindowSize(inner_index->getMaxError());
    outer_iterator->seekTo(outer_start);

    while (outer_iterator->currentPos() < outer_end) {
      auto bounds = inner_index->getPositionBoundsRA(outer_iterator->key());
      SearchResult result;
      do {
        auto window = inner_iterator->getWindow(bounds.lower, bounds.upper);
        result = search_strategy_->search(window, outer_iterator->key(), bounds,
                                          this->comparator_);
        bounds.lower =
            window.hi_idx; // If you search next time, search from here.
        if (bounds.lower == inner_end)
          break; // If no more items to read, break.
      } while (result.shouldContinue);
      if (result.found) {
        result_builder->add(outer_iterator->key());
      }
      last_found_idx = result.lower_bound; // Never search before this again.
      outer_iterator->next();
    }
    result->stats["inner_disk_fetch"] = inner_iterator->getDiskFetches();
    result->stats["inner_disk_fetch_size"] = inner_iterator->getDiskFetchSize();
    result->stats["inner_total_bytes_fetched"] =
        inner_iterator->getTotalBytesFetched();
    result->stats["outer_disk_fetch"] = outer_iterator->getDiskFetches();
    result->stats["outer_disk_fetch_size"] = outer_iterator->getDiskFetchSize();
    result->stats["outer_total_bytes_fetched"] =
        outer_iterator->getTotalBytesFetched();
    result->output_table = result_builder->build(), delete outer_iterator;
    delete inner_iterator;
  }

private:
  SearchStrategy<T> *search_strategy_;
};

// TODO: Generalize to all types.
class HashJoin : public BaseMergeAndJoinOp<KVSlice> {
public:
  HashJoin(SSTable<KVSlice> *outer, SSTable<KVSlice> *inner,
           Index<KVSlice> *inner_index, Comparator<KVSlice> *comparator,
           PSSTableBuilder<KVSlice> *result_builder, int num_threads)
      : BaseMergeAndJoinOp<KVSlice>(outer, inner, inner_index, comparator,
                                    result_builder, num_threads) {}

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

  void doOpOnPartition(Partition partition,
                       TableOpResult<KVSlice> *result) override {
    uint64_t outer_start = partition.outer.first;
    uint64_t outer_end = partition.outer.second;
    uint64_t inner_start = partition.inner.first;
    uint64_t inner_end = partition.inner.second;

    auto inner_iterator = this->inner_->iterator();
    auto outer_iterator = this->outer_->iterator();
    auto result_builder = this->result_builder_->getBuilderForRange(
        inner_start + outer_start, inner_end + outer_end);

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
    result->stats["inner_disk_fetch_size"] = inner_iterator->getDiskFetchSize();
    result->stats["inner_total_bytes_fetched"] =
        inner_iterator->getTotalBytesFetched();
    result->stats["outer_disk_fetch"] = outer_iterator->getDiskFetches();
    result->stats["outer_disk_fetch_size"] = outer_iterator->getDiskFetchSize();
    result->stats["outer_total_bytes_fetched"] =
        outer_iterator->getTotalBytesFetched();
    result->output_table = result_builder->build();
  }

private:
  std::unordered_map<std::string, uint64_t> outer_hash_map;
};

template <class T> class SortJoin : public BaseMergeAndJoinOp<T> {
public:
  SortJoin(SSTable<T> *outer, SSTable<T> *inner, Index<T> *inner_index,
           Comparator<T> *comparator, PSSTableBuilder<T> *result_builder,
           int num_threads)
      : BaseMergeAndJoinOp<T>(outer, inner, inner_index, comparator,
                              result_builder, num_threads) {}

  void doOpOnPartition(Partition partition, TableOpResult<T> *result) override {
    uint64_t outer_start = partition.outer.first;
    uint64_t outer_end = partition.outer.second;
    uint64_t inner_start = partition.inner.first;
    uint64_t inner_end = partition.inner.second;

    auto outer_iterator = this->outer_->iterator();
    auto inner_iterator = this->inner_->iterator();
    auto result_builder = this->result_builder_->getBuilderForRange(
        inner_start + outer_start, inner_end + outer_end);
    outer_iterator->seekTo(outer_start);
    inner_iterator->seekTo(inner_start);

    while (outer_iterator->currentPos() < outer_end) {
      while (inner_iterator->currentPos() < inner_end &&
             (this->comparator_->compare(inner_iterator->key(),
                                         outer_iterator->key()) < 0)) {
        inner_iterator->next();
      }
      if (!(inner_iterator->currentPos() < inner_end)) {
        break;
      }
      if (this->comparator_->compare(outer_iterator->key(),
                                     inner_iterator->key()) == 0) {
        result_builder->add(inner_iterator->key());
      }
      outer_iterator->next();
    }

    result->stats["inner_disk_fetch"] = inner_iterator->getDiskFetches();
    result->stats["inner_disk_fetch_size"] = inner_iterator->getDiskFetchSize();
    result->stats["inner_total_bytes_fetched"] =
        inner_iterator->getTotalBytesFetched();
    result->stats["outer_disk_fetch"] = outer_iterator->getDiskFetches();
    result->stats["outer_disk_fetch_size"] = outer_iterator->getDiskFetchSize();
    result->stats["outer_total_bytes_fetched"] =
        outer_iterator->getTotalBytesFetched();
    result->output_table = result_builder->build(),

    delete outer_iterator;
    delete inner_iterator;
  }
};

} // namespace li_merge

#endif

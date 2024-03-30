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
#include <algorithm>
#include <unordered_set>
// TODO(chesetti): Must be last for some reason. Fix.
#include <nlohmann/json.hpp>
#include "table_op.h"
#include "b_tree.h"
#include "pgm/pgm_index.hpp"
#include "pgm/pgm_index_variants.hpp"

using json = nlohmann::json;
typedef std::pair<uint64_t, uint64_t> pii;

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
  // inner is the smaller table. outer is the larger table table.
  LearnedSortJoinOnUnsortedData(SSTable<T> *inner, SSTable<T> *outer,
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
    std::sort(sampled_keys.begin(), sampled_keys.end());
    auto index = new pgm::PGMIndex<uint64_t, 1>(sampled_keys);

    // Calculate buckets 
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
  // inner is the smaller table. outer is the larger table table.
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

template <class T> class LearnedSortJoinOnUnsortedDataSortedOutput: public TableOp<T> {
  std::string outer_index_file_;
  std::string inner_index_file_;
  public:
  LearnedSortJoinOnUnsortedDataSortedOutput(SSTable<T> *outer, SSTable<T> *inner,
                  PSSTableBuilder<T> *result_builder, int num_threads, std::string outer_index_file, std::string inner_index_file)
      : TableOp<T>(outer, inner, result_builder, num_threads), outer_index_file_(outer_index_file), inner_index_file_(inner_index_file) {}
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
    uint64_t bytes_read = 0;
    uint64_t outer_start = partition.outer.first;
    uint64_t outer_end = partition.outer.second;
    uint64_t inner_start = partition.inner.first;
    uint64_t inner_end = partition.inner.second;
    auto result_builder = this->result_builder_->getBuilderForRange(
        inner_start + outer_start, inner_end + outer_end);

    // Build the CDF for outer.
    std::vector<uint64_t> outer_sampled_keys;
    uint64_t outer_num_keys = outer_end - outer_start;
    uint64_t sample_freq = 100;
    uint64_t sample_step = outer_num_keys / sample_freq;
    auto outer_iter = this->outer_->iterator();
    auto inner_iter = this->inner_->iterator();
    for (uint64_t i=outer_start; i < outer_end; i+=sample_freq) {
      outer_iter->seekTo(i);
      KVSlice k = outer_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      outer_sampled_keys.push_back(key);
    }
    std::sort(outer_sampled_keys.begin(), outer_sampled_keys.end());
    auto outer_index = new pgm::PGMIndex<uint64_t, 1>(outer_sampled_keys);

    // Calculate buckets 
    uint64_t outer_num_buckets = outer_num_keys / (256) + 64;
    uint64_t outer_max_key = 0;
    uint64_t *outer_bucket_sizes = new uint64_t[outer_num_buckets + 64];
    uint64_t *outer_bucket_prefix = new uint64_t[outer_num_buckets + 64];
    uint64_t *outer_bucket_max = new uint64_t[outer_num_buckets + 64];
    uint64_t *outer_bucket_cur = new uint64_t[outer_num_buckets + 64];
    memset((void *)outer_bucket_sizes, 0, sizeof(uint64_t) * (outer_num_buckets + 64));
    memset((void *)outer_bucket_prefix, 0, sizeof(uint64_t) * (outer_num_buckets + 64));
    memset((void *)outer_bucket_max, 0, sizeof(uint64_t) * (outer_num_buckets + 64));
    memset((void *)outer_bucket_cur, 0, sizeof(uint64_t) * (outer_num_buckets + 64));

    char kvbuf[16];

    // Write the buckets 
    outer_iter->seekTo(0);
    for (uint64_t i=outer_start; i < outer_end; i++) {
      KVSlice k = outer_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      outer_max_key = std::max(outer_max_key, key);
      auto pos = outer_index->search(key).pos;
      // pos is scaled to [0, num_keys/100]
      uint64_t bucket = (pos * 1.0 * sample_freq / outer_num_keys) * outer_num_buckets;
      if (bucket >= outer_num_buckets + 64) {
        abort();
      }
      outer_bucket_sizes[bucket]++;
      outer_bucket_max[bucket] = std::max(outer_bucket_max[bucket], key);
      outer_iter->next();
    }
    uint64_t outer_max_buffer_size = outer_bucket_sizes[0];
    for (uint64_t i=1; i<outer_num_buckets + 64; i++)  {
      outer_bucket_prefix[i] = outer_bucket_prefix[i-1] + outer_bucket_sizes[i-1];
      outer_bucket_max[i] = std::max(outer_bucket_max[i], outer_bucket_max[i-1]);
      outer_max_buffer_size = std::max(outer_bucket_sizes[i], outer_max_buffer_size);
      // printf("%lld %lld %lld\n", outer_bucket_max[i], outer_bucket_prefix[i], outer_bucket_sizes[i]);
    }
    int fd = open(outer_index_file_.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd == -1) {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }
    outer_iter->seekTo(0);
    for (uint64_t i=outer_start; i<outer_end; i++) {
      KVSlice k = outer_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      auto pos = outer_index->search(key).pos;
      // pos is scaled to [0, num_keys/100]
      uint64_t b= (pos * 1.0 * sample_freq / outer_num_keys) * outer_num_buckets;
      if (b >= outer_num_buckets + 64) {
        abort();
      }
      uint64_t offset = (outer_bucket_prefix[b] + outer_bucket_cur[b]) * (2 * sizeof(uint64_t));
      memcpy(kvbuf, k.data(), 8);
      memcpy(kvbuf + 8, (char *)(&i), 8);
      pwrite(fd, kvbuf, 2 * sizeof(uint64_t), offset);
      outer_bucket_cur[b]++;
      outer_iter->next();
    }
    close(fd);

    // Now do the same for inner.
    // Build the CDF for inner.
    std::vector<uint64_t> inner_sampled_keys;
    uint64_t inner_num_keys = inner_end - inner_start;
    sample_step = inner_num_keys / sample_freq;
    for (uint64_t i=inner_start; i < inner_end; i+=sample_freq) {
      inner_iter->seekTo(i);
      KVSlice k = inner_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      inner_sampled_keys.push_back(key);
    }
    std::sort(inner_sampled_keys.begin(), inner_sampled_keys.end());
    auto inner_index = new pgm::PGMIndex<uint64_t, 1>(inner_sampled_keys);

    // Calculate buckets 
    uint64_t inner_num_buckets = inner_num_keys / (256) + 64;
    uint64_t inner_max_key = 0;
    uint64_t *inner_bucket_sizes = new uint64_t[inner_num_buckets + 64];
    uint64_t *inner_bucket_prefix = new uint64_t[inner_num_buckets + 64];
    uint64_t *inner_bucket_max = new uint64_t[inner_num_buckets + 64];
    uint64_t *inner_bucket_cur = new uint64_t[inner_num_buckets + 64];
    memset((void *)inner_bucket_sizes, 0, sizeof(uint64_t) * (inner_num_buckets + 64));
    memset((void *)inner_bucket_prefix, 0, sizeof(uint64_t) * (inner_num_buckets + 64));
    memset((void *)inner_bucket_max, 0, sizeof(uint64_t) * (inner_num_buckets + 64));
    memset((void *)inner_bucket_cur, 0, sizeof(uint64_t) * (inner_num_buckets + 64));

    // Write the buckets 
    inner_iter->seekTo(0);
    for (uint64_t i=inner_start; i < inner_end; i++) {
      KVSlice k = inner_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      inner_max_key = std::max(inner_max_key, key);
      auto pos = inner_index->search(key).pos;
      // pos is scaled to [0, num_keys/100]
      uint64_t bucket = (pos * 1.0 * sample_freq / inner_num_keys) * inner_num_buckets;
      if (bucket >= inner_num_buckets + 64) {
        abort();
      }
      inner_bucket_sizes[bucket]++;
      inner_bucket_max[bucket] = std::max(inner_bucket_max[bucket], key);
      inner_iter->next();
    }
    uint64_t inner_max_buffer_size = inner_bucket_sizes[0];
    for (uint64_t i=1; i<inner_num_buckets + 64; i++)  {
      inner_bucket_prefix[i] = inner_bucket_prefix[i-1] + inner_bucket_sizes[i-1];
      inner_bucket_max[i] = std::max(inner_bucket_max[i], inner_bucket_max[i-1]);
      inner_max_buffer_size = std::max(inner_bucket_sizes[i], inner_max_buffer_size);
    }

    fd = open(inner_index_file_.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd == -1) {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }
    inner_iter->seekTo(0);
    for (uint64_t i=inner_start; i<inner_end; i++) {
      KVSlice k = inner_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      auto pos = inner_index->search(key).pos;
      // pos is scaled to [0, num_keys/100]
      uint64_t b= (pos * 1.0 * sample_freq / inner_num_keys) * inner_num_buckets;
      if (b >= inner_num_buckets + 64) {
        abort();
      }
      uint64_t offset = (inner_bucket_prefix[b] + inner_bucket_cur[b]) * (2 * sizeof(uint64_t));
      memcpy(kvbuf, k.data(), 8);
      memcpy(kvbuf + 8, (char *)(&i), 8);
      pwrite(fd, kvbuf, 2 * sizeof(uint64_t), offset);
      inner_bucket_cur[b]++;
      inner_iter->next();
    }
    close(fd);

    int fd1 = open(inner_index_file_.c_str(), O_RDONLY, 0644);
    int fd2 = open(outer_index_file_.c_str(), O_RDONLY, 0644);
    uint64_t inner_bucket = 0, inner_pos = 0;
    uint64_t outer_bucket = 0, outer_pos = 0;
    pii *inner_buffer = new pii[inner_max_buffer_size];
    pii *outer_buffer = new pii[outer_max_buffer_size];
    
    memset(kvbuf, 0, 16);

    pread(fd1, (void *)inner_buffer, inner_bucket_sizes[inner_bucket] * sizeof(pii), inner_bucket_prefix[inner_bucket] * sizeof(pii));
    std::sort(inner_buffer, inner_buffer + inner_bucket_sizes[inner_bucket]);
    pread(fd2, (void *)outer_buffer, outer_bucket_sizes[outer_bucket] * sizeof(pii), outer_bucket_prefix[outer_bucket] * sizeof(pii));
    std::sort(outer_buffer, outer_buffer + outer_bucket_sizes[outer_bucket]);
    bytes_read += (outer_bucket_sizes[outer_bucket] + inner_bucket_sizes[inner_bucket]) * sizeof(pii);

    pii inner_key(0,0);
    pii outer_key(0,0);
    uint64_t current_inner_bucket = -1;
    uint64_t inner_buffer_pos = 0;

    while (outer_pos < outer_num_keys) {
      uint64_t outer_buffer_offset = outer_pos - outer_bucket_prefix[outer_bucket];
      outer_key = outer_buffer[outer_buffer_offset];

      auto pos = inner_index->search(outer_key.first).pos;
      uint64_t inner_bucket = (pos * 1.0 * sample_freq / inner_num_keys) * inner_num_buckets;
      if (inner_bucket != current_inner_bucket) {
        pread(fd1, (void *)inner_buffer, inner_bucket_sizes[inner_bucket] * sizeof(pii), inner_bucket_prefix[inner_bucket] * sizeof(pii));
        bytes_read += (inner_bucket_sizes[inner_bucket]) * sizeof(pii);
        std::sort(inner_buffer, inner_buffer + inner_bucket_sizes[inner_bucket]);
        current_inner_bucket = inner_bucket;
        inner_buffer_pos= 0;
      }

      while(true) {
        inner_key = inner_buffer[inner_buffer_pos];
        if (inner_key.first >= outer_key.first) {
          break;
        }
        inner_buffer_pos++;
        if (inner_buffer_pos == inner_bucket_sizes[inner_bucket]) {
          if (inner_bucket >= inner_num_buckets) break;
          while (inner_buffer_pos == inner_bucket_sizes[inner_bucket]) {
            inner_bucket++;
            pread(fd1, (void *)inner_buffer, inner_bucket_sizes[inner_bucket] * sizeof(pii), inner_bucket_prefix[inner_bucket] * sizeof(pii));
            bytes_read += (inner_bucket_sizes[inner_bucket]) * sizeof(pii);
            std::sort(inner_buffer, inner_buffer + inner_bucket_sizes[inner_bucket]);
            current_inner_bucket = inner_bucket;
            inner_buffer_pos= 0;
          }
        }
      }

      if (outer_key.first == inner_key.first) {
        inner_iter->seekTo(inner_key.second);
        result_builder->add(inner_iter->key());
      }

      outer_pos++;
      while (outer_pos == outer_bucket_prefix[outer_bucket+1]) {
        outer_bucket++;
        pread(fd2, (void *)outer_buffer, outer_bucket_sizes[outer_bucket] * sizeof(pii), outer_bucket_prefix[outer_bucket] * sizeof(pii));
          bytes_read += (outer_bucket_sizes[outer_bucket]) * sizeof(pii);
        std::sort(outer_buffer, outer_buffer + outer_bucket_sizes[outer_bucket]);
      }
    }


#if 0
    while (inner_pos < inner_num_keys && outer_pos < outer_num_keys) {
      uint64_t inner_buffer_offset = inner_pos - inner_bucket_prefix[inner_bucket];
      uint64_t outer_buffer_offset = outer_pos - outer_bucket_prefix[outer_bucket];

      if (inner_buffer[inner_buffer_offset] < inner_last_key) {
        abort();
      }
      if (outer_buffer[outer_buffer_offset] < outer_last_key) {
        abort();
      }
      inner_last_key = inner_buffer[inner_buffer_offset];
      outer_last_key = outer_buffer[outer_buffer_offset];

      if (inner_buffer[inner_buffer_offset] == outer_buffer[outer_buffer_offset] ) {
        memcpy(kvbuf, (char *)(inner_buffer + inner_buffer_offset), 8);
        KVSlice k(kvbuf, 8, 8);
        result_builder->add(k);
        inner_pos++;
        outer_pos++;
      }
      else if (inner_buffer[inner_buffer_offset] < outer_buffer[outer_buffer_offset]) {
        inner_pos++;
      } else {
        outer_pos++;
      }
      while (inner_pos == inner_bucket_prefix[inner_bucket+1]) {
        inner_bucket++;
        pread(fd1, (void *)inner_buffer, inner_bucket_sizes[inner_bucket] * 8, inner_bucket_prefix[inner_bucket] * 8);
        std::sort(inner_buffer, inner_buffer + inner_bucket_sizes[inner_bucket]);
      }
      while (outer_pos == outer_bucket_prefix[outer_bucket+1]) {
        outer_bucket++;
        pread(fd2, (void *)outer_buffer, outer_bucket_sizes[outer_bucket] * 8, outer_bucket_prefix[outer_bucket] * 8);
        std::sort(outer_buffer, outer_buffer + outer_bucket_sizes[outer_bucket]);
      }
    }
    #endif

    close(fd1);
    close(fd2);

    result->stats["bucket_size"] = std::max(inner_max_buffer_size, outer_max_buffer_size);
    result->stats["disk_read"] = inner_iter->getTotalBytesFetched() + outer_iter->getTotalBytesFetched() + bytes_read; 
    result->stats["disk_write"] = (outer_end-outer_start + inner_end-inner_start) * sizeof(pii);

    result->output_table = result_builder->build();
  }

  void mergePartitions() override {
    this->stats_["bucket_size"] = this->partition_results_[0].stats["bucket_size"];
    this->stats_["disk_read"] = this->partition_results_[0].stats["disk_read"];
    this->stats_["disk_write"] = this->partition_results_[0].stats["disk_write"];
    this->output_table_ = this->result_builder_->build();
  }
};

template <class T> class IndexedJoinOnUnsortedDataSortedOutput: public TableOp<T> {
  public:
  std::string inner_btree_file_path_;
  std::string outer_btree_file_path_;
  void postOp() override {
    // std::remove(btree_file_path_.c_str());
  }
  IndexedJoinOnUnsortedDataSortedOutput(SSTable<T> *outer, SSTable<T> *inner,
                  PSSTableBuilder<T> *result_builder, int num_threads, std::string inner_btree_file_path, std::string outer_btree_file_path)
      : TableOp<T>(outer, inner, result_builder, num_threads), inner_btree_file_path_(inner_btree_file_path), outer_btree_file_path_(outer_btree_file_path) {}

  std::vector<Partition> getPartitions() override {
    if (this->num_threads_ != 1) {
      abort();
    }
    uint64_t outer_start = 0;
    uint64_t outer_end =  this->outer_->iterator()->numElts();
    uint64_t inner_start = 0;
    uint64_t inner_end =  this->inner_->iterator()->numElts();
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

    // std::vector<uint64_t> inner_keys;
    // std::vector<uint64_t> outer_keys;

    char *outerBtreeName = new char[outer_btree_file_path_.size() + 1];
    memset(outerBtreeName, '\0', outer_btree_file_path_.size() + 1);
    memcpy(outerBtreeName, outer_btree_file_path_.c_str(), outer_btree_file_path_.size());
    BTree *outer_btree = new BTree(LEAF_DISK, true, outerBtreeName);
    auto outer_iter = this->outer_->iterator();
    for (uint64_t i=outer_start; i<outer_end; i++) {
      KVSlice k = outer_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      lni.key = key; lni.value = i;
      // printf("%lld %lld\n", lni.key, lni.value);
      // outer_keys.push_back(key);
      outer_btree->insert_key_leaf_disk(lni);
      outer_iter->next();
    }

    char *innerBtreeName = new char[inner_btree_file_path_.size() + 1];
    memset(innerBtreeName, '\0', inner_btree_file_path_.size() + 1);
    memcpy(innerBtreeName, inner_btree_file_path_.c_str(), inner_btree_file_path_.size());
    BTree *inner_btree = new BTree(LEAF_DISK, true, innerBtreeName);
    auto inner_iter = this->inner_->iterator();
    for (uint64_t i=inner_start; i<inner_end; i++) {
      KVSlice k = inner_iter->key();
      uint64_t key = *(uint64_t *)k.data();
      lni.key = key; lni.value = i;
      // printf("%lld %lld\n", lni.key, lni.value);
      // inner_keys.push_back(key);
      inner_btree->insert_key_leaf_disk(lni);
      inner_iter->next();
    }


    // std::sort(inner_keys.begin(), inner_keys.end());
    // std::sort(outer_keys.begin(), outer_keys.end());
    /*
    uint64_t missing_inner = 0, missing_outer=0;
    for (auto k: inner_keys) {
      uint64_t pos;
      if (!inner_btree->lookup_value(k, &pos, &c)){
        missing_inner++;
      }
    }
    for (auto k: outer_keys) {
      uint64_t pos;
      if (!outer_btree->lookup_value(k, &pos, &c)){
        missing_outer++;
      }
    }
    printf("missing %lld %lld\n", missing_inner, missing_outer);
    printf("end %lld %lld\n", inner_end, outer_end);
    */

#if 0
    // auto inner_it = inner_btree->inner_btree.begin(); 
    auto inner_it = inner_btree->inner_btree.find_for_disk(0);
    int block_id = inner_it.data();
    auto vit = inner_keys.begin();
    printf("last block: %lld block_count: %lld level: %lld\n", inner_btree->metanode.last_block, inner_btree->metanode.block_count, inner_btree->metanode.level);
    while (inner_it != inner_btree->inner_btree.end()) {
      Block inner_block = inner_btree->sm->get_block(inner_it.data());
      LeaftNodeHeader *inner_head = (LeaftNodeHeader *) (inner_block.data);
      LeafNodeIterm *inner_items = (LeafNodeIterm *) (inner_block.data + LeaftNodeHeaderSize);
      inner_it++;
      uint64_t mn, mx;
      mn = inner_items[0].key;
      mx = inner_items[0].key;
      for (uint64_t i=0; i < inner_head->item_count; i++) {
        mn = std::min(mn, inner_items[i].key);
        mx = std::max(mx, inner_items[i].key);
        if (inner_items[i].key != *(vit)) {
          printf("Missing key %lld %lld!\n", *(vit), inner_items[i].key);
          uint64_t pos; 
          bool found = inner_btree->lookup_value(*(vit), &pos, &c);
          printf("%d\n", found);
        }
        while (*(vit) <= inner_items[i].key && vit!=inner_keys.end()) {
          vit++;
        }
      }
      printf("%lld (%lld %lld) NodeType:%lld NextBlock:%lld, Level: %lld [%lld %lld]\n", block_id, inner_it.key(), inner_it.data(), inner_head->node_type, inner_head->next_block_id, inner_head->level, mn,mx);
      if (block_id == inner_btree->metanode.last_block) {
        break;
      }
      block_id = inner_head->next_block_id;
    }
    #endif

    auto outer_it = outer_btree->inner_btree.begin(); 
    Block outer_block = outer_btree->sm->get_block(outer_it.data());

    LeaftNodeHeader *outer_head = (LeaftNodeHeader *) (outer_block.data);
    LeafNodeIterm *outer_items = (LeafNodeIterm *) (outer_block.data + LeaftNodeHeaderSize);
    int outer_is_last = 0;
    uint64_t oc=0;

    char kvbuf[16];
    memset(kvbuf, 0, 16);

    uint64_t val;
    while (true) {
      uint64_t okey = outer_items[oc].key;
      if (inner_btree->lookup_value(okey, &val, &c) ) {
        inner_iter->seekTo(val);
        result_builder->add(inner_iter->key());
      }

      oc++;
      if (oc == outer_head->item_count) {
        outer_it++;
        if (outer_is_last) break;
        if (outer_it == outer_btree->inner_btree.end()) {
          outer_block = outer_btree->sm->get_block(outer_btree->metanode.last_block);
          outer_is_last = 1;
          // printf("%lld %lld\n", inner_count, outer_count);
        } else {
          outer_block = outer_btree->sm->get_block(outer_it.data());
        };
        oc = 0;
        outer_head = (LeaftNodeHeader *) (outer_block.data);
        outer_items = (LeafNodeIterm *) (outer_block.data + LeaftNodeHeaderSize);
      }
    }

#if 0
    auto outer_it = outer_btree->inner_btree.begin(); 
    auto inner_it = inner_btree->inner_btree.begin(); 
    Block inner_block = inner_btree->sm->get_block(inner_it.data());
    Block outer_block = outer_btree->sm->get_block(outer_it.data());

    LeaftNodeHeader *inner_head = (LeaftNodeHeader *) (inner_block.data);
    LeafNodeIterm *inner_items = (LeafNodeIterm *) (inner_block.data + LeaftNodeHeaderSize);
    LeaftNodeHeader *outer_head = (LeaftNodeHeader *) (outer_block.data);
    LeafNodeIterm *outer_items = (LeafNodeIterm *) (outer_block.data + LeaftNodeHeaderSize);
    uint64_t ic=0;
    uint64_t oc=0;
    char kvbuf[16];
    memset(kvbuf, 0, 16);
    int inner_is_last = 0;
    int outer_is_last = 0;
    uint64_t inner_count = 0;
    uint64_t outer_count = 0;
    while (true) {
      uint64_t ikey = inner_items[ic].key;
      uint64_t okey = outer_items[oc].key;
      if (ikey == okey) {
        memcpy(kvbuf, (char *)(&ikey), 8);
        KVSlice k(kvbuf, 8, 8);
        result_builder->add(k);
        ic++;
        oc++;
        inner_count++;
        outer_count++;
      } else if (ikey < okey) {
        ic++;
        inner_count++;
      } else {
        oc++;
        outer_count++;
      }

      if (ic == inner_head->item_count) {
        inner_it++;
        if (inner_is_last) break;
        if (inner_it == inner_btree->inner_btree.end()) {
          inner_block = inner_btree->sm->get_block(inner_btree->metanode.last_block);
          inner_is_last = 1;
          // printf("count: %lld %lld\n", inner_count, outer_count);
        } else {
          inner_block = inner_btree->sm->get_block(inner_it.data());
        }
        ic = 0;
        inner_head = (LeaftNodeHeader *) (inner_block.data);
        inner_items = (LeafNodeIterm *) (inner_block.data + LeaftNodeHeaderSize);
      }
      if (oc == outer_head->item_count) {
        outer_it++;
        if (outer_is_last) break;
        if (outer_it == outer_btree->inner_btree.end()) {
          outer_block = outer_btree->sm->get_block(outer_btree->metanode.last_block);
          outer_is_last = 1;
          // printf("%lld %lld\n", inner_count, outer_count);
        } else {
          outer_block = outer_btree->sm->get_block(outer_it.data());
        };
        oc = 0;
        outer_head = (LeaftNodeHeader *) (outer_block.data);
        outer_items = (LeafNodeIterm *) (outer_block.data + LeaftNodeHeaderSize);
      }
    }
    // printf("final: %lld %lld\n", inner_count, outer_count);
    #endif

#if 0

    inner_index_iter.has_next_range(&c);
    outer_index_iter.has_next_range(&c);
    uint64_t inner_key = 0;
    uint64_t outer_key = 0;
    while (ic < inner_end && oc < outer_end) {
    // while (inner_index_iter.has_next() && outer_index_iter.has_next()) {
      if (inner_key <= inner_index_iter.key()) {
        inner_key = inner_index_iter.key();
      } else {
        abort();
      }
      if (outer_key <= outer_index_iter.key()) {
        outer_key = outer_index_iter.key();
      } else {
        abort();
      }
      // printf("[%lld %lld] [%lld %lld]\n", inner_key, inner_index_iter.value(), outer_key, outer_index_iter.value());
      if (inner_key == outer_key) {
        memcpy(kvbuf, (char *)(&inner_key), 8);
        KVSlice k(kvbuf, 8, 8);
        result_builder->add(k);
        inner_index_iter.has_next_range(&c);
        inner_index_iter.next();
        outer_index_iter.has_next_range(&c);
        outer_index_iter.next();
        oc++;
        ic++;
      } else if (inner_key < outer_key) {
        inner_index_iter.has_next_range(&c);
        inner_index_iter.next();
        ic++;
      } else {
        outer_index_iter.has_next_range(&c);
        inner_index_iter.has_next();
        outer_index_iter.next();
        oc++;
      }
    }
  #endif
    result->stats["disk_read"] = inner_btree->bytes_read() + outer_btree->bytes_read() + inner_iter->getTotalBytesFetched() + outer_iter->getTotalBytesFetched(); 
    result->stats["disk_write"] = inner_btree->bytes_written() + outer_btree->bytes_written() + (outer_end-outer_start + inner_end-inner_start) * sizeof(pii);
    delete inner_btree;
    delete outer_btree;
    result->output_table = result_builder->build();
  }

  void mergePartitions() override {
    this->stats_["disk_read"] = this->partition_results_[0].stats["disk_read"];
    this->stats_["disk_write"] = this->partition_results_[0].stats["disk_write"];
    this->stats_["bucket_size"] = 256;
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

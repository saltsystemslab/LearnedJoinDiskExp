#ifndef TABLE_OP_H 
#define TABLE_OP_H

namespace li_merge {

#include "sstable.h"
#include "partition.h"
#include <nlohmann/json.hpp>
#include <vector>
#include <thread>
#include <chrono>
using json = nlohmann::json;

template <class T>
struct TableOpResult {
    SSTable<T> *output_table;
    json stats;
};

template <class T>
class TableOp {
public:
  virtual void preOp() = 0;
  virtual std::vector<Partition> getPartitions() = 0;
  virtual void doOpOnPartition(Partition partition, TableOpResult<T> *result) = 0;
  virtual void mergePartitions() = 0;
  TableOp(
      SSTable<T> *outer, 
      SSTable<T> *inner,
      PSSTableBuilder<T> *result_builder,
      int num_threads) : 
    inner_(inner), outer_(outer), 
    result_builder_(result_builder), 
    num_threads_(num_threads),
    partition_results_(std::vector<TableOpResult<T>>(num_threads)) {}

  TableOpResult<T> profileOp() {
    preOp();
    std::vector<Partition> partitions = getPartitions();
    std::vector<std::thread> threads;
    auto op_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < num_threads_; i++) {
       //threads.push_back(std::thread(&TableOp::doOpOnPartition, this, partitions[i], &partition_results_[i]));
       doOpOnPartition(partitions[i], &partition_results_[i]);
    }
    for (int i = 0; i < num_threads_; i++) {
      // threads[i].join();
    }
    mergePartitions();
    auto op_end = std::chrono::high_resolution_clock::now();
    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         op_end - op_start).count();
    stats_["duration_ns"] = duration_ns;
    stats_["duration_sec"] = duration_ns / 1e9;
    return TableOpResult<T> {
      output_table_,
      stats_
    };
  }
protected:
  SSTable<T> *outer_;
  SSTable<T> *inner_;
  SSTable<T> *output_table_;
  PSSTableBuilder<T> *result_builder_;
  std::vector<TableOpResult<T>> partition_results_;
  int num_threads_;
  json stats_;
};

}

#endif

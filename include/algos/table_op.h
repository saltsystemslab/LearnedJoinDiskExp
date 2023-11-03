#ifndef TABLE_OP_H 
#define TABLE_OP_H

namespace li_merge {

#include "sstable.h"
#include <nlohmann/json.hpp>
#include <chrono>
using json = nlohmann::json;


template <class T>
class TableOp {
public:
  struct TableOpResult {
    SSTable<T> *output_table;
    json stats;
  };
  virtual void preOp() {};
  virtual SSTable<T> *doOp() = 0;
  virtual void postOp() {};
  TableOp(
      SSTable<T> *outer, 
      SSTable<T> *inner, 
      SSTableBuilder<T> *result_builder
    ) : 
    inner_(inner), outer_(outer), result_builder_(result_builder) {}

  TableOpResult profileOp() {
    preOp();
    auto op_start = std::chrono::high_resolution_clock::now();
    SSTable<T> *outputTable = doOp();
    auto op_end = std::chrono::high_resolution_clock::now();
    auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         op_end - op_start).count();
    stats_["duration_ns"] = duration_ns;
    stats_["duration_sec"] = duration_ns / 1e9;
    postOp();
    return TableOpResult {
      outputTable,
      stats_
    };
  }
protected:
  SSTable<T> *outer_;
  SSTable<T> *inner_;
  SSTableBuilder<T> *result_builder_;
  json stats_;
};

}

#endif

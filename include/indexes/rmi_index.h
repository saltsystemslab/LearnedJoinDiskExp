#ifndef LEARNEDINDEXMERGE_RMI_INDEX_H
#define LEARNEDINDEXMERGE_RMI_INDEX_H

#include "config.h"
#include "index.h"
#include "key_to_point.h"
#include "key_value_slice.h"
#include "pgm/pgm_index.hpp"
#include "pgm/pgm_index_variants.hpp"
#include "rs/multi_map.h"
#include "rs/serializer.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <vector>

#define RMI_DATA "/home/chesetti/Repos/LearnedJoinDiskExp/SOSD/rmi_data"

#include "fb_200M_uint64_0.h"
#include "osm_cellids_800M_uint64_0.h"
#include "books_800M_uint64_0.h"
#include "wiki_ts_200M_uint64_1.h"
#include "uniform_dense_200M_uint64_8.h"
#include "uniform_sparse_200M_uint64_0.h"
#include "lognormal_200M_uint64_2.h"
#include "normal_200M_uint64_3.h"

namespace li_merge {

template <class T> class RmiIndex : public Index<T> {
public:
  RmiIndex(std::string dataset, KeyToPointConverter<T> *converter) : converter_(converter) {
    if (dataset=="fb_200M_uint64") {
        fb_200M_uint64_0::load(RMI_DATA);
        lookup = fb_200M_uint64_0::lookup;
        build_duration = fb_200M_uint64_0::BUILD_TIME_NS;
        size_in_bytes = fb_200M_uint64_0::RMI_SIZE;
    } else if (dataset=="osm_cellids_800M_uint64") {
        osm_cellids_800M_uint64_0::load(RMI_DATA);
        lookup = osm_cellids_800M_uint64_0::lookup;
        build_duration = osm_cellids_800M_uint64_0::BUILD_TIME_NS;
        size_in_bytes = osm_cellids_800M_uint64_0::RMI_SIZE;
    } else if (dataset=="books_800M_uint64") {
        books_800M_uint64_0::load(RMI_DATA);
        lookup = books_800M_uint64_0::lookup;
        build_duration = books_800M_uint64_0::BUILD_TIME_NS;
        size_in_bytes = books_800M_uint64_0::RMI_SIZE;
    } else if (dataset=="wiki_ts_200M_uint64"){
        wiki_ts_200M_uint64_1::load(RMI_DATA);
        lookup = wiki_ts_200M_uint64_1::lookup;
        build_duration = wiki_ts_200M_uint64_1::BUILD_TIME_NS;
        size_in_bytes = wiki_ts_200M_uint64_1::RMI_SIZE;
    } else if (dataset=="uniform_dense_200M_uint64"){
        uniform_dense_200M_uint64_8::load(RMI_DATA);
        lookup = uniform_dense_200M_uint64_8::lookup;
        build_duration = uniform_dense_200M_uint64_8::BUILD_TIME_NS;
        size_in_bytes = uniform_dense_200M_uint64_8::RMI_SIZE;
    } else if (dataset=="uniform_sparse_200M_uint64"){
        uniform_sparse_200M_uint64_0::load(RMI_DATA);
        lookup = uniform_sparse_200M_uint64_0::lookup;
        build_duration = uniform_sparse_200M_uint64_0::BUILD_TIME_NS;
        size_in_bytes = uniform_sparse_200M_uint64_0::RMI_SIZE;
    } else if (dataset=="lognormal_200M_uint64"){
        lognormal_200M_uint64_2::load(RMI_DATA);
        lookup = lognormal_200M_uint64_2::lookup;
        build_duration = lognormal_200M_uint64_2::BUILD_TIME_NS;
        size_in_bytes = lognormal_200M_uint64_2::RMI_SIZE;
    } else if (dataset=="normal_200M_uint64"){
        normal_200M_uint64_3::load(RMI_DATA);
        lookup = normal_200M_uint64_3::lookup;
        build_duration = normal_200M_uint64_3::BUILD_TIME_NS;
        size_in_bytes = normal_200M_uint64_3::RMI_SIZE;
    } 
    else {
      abort();
    }
  }
  Bounds getPositionBoundsRA(const T &t) override {
    uint64_t err;
    uint64_t pos = lookup(converter_->toPoint(t), &err); 
    uint64_t lo = (pos > err) ? pos - err : 0;
    return Bounds{lo, pos+err, 0};
  }
  Bounds getPositionBounds(const T &t) override { 
    uint64_t err;
    uint64_t pos = lookup(converter_->toPoint(t), &err); 
    uint64_t lo = (pos > err) ? pos - err : 0;
    return Bounds{lo, pos+err, 0};
  }
  uint64_t sizeInBytes() override { 
    return size_in_bytes; 
  }
  uint64_t buildDuration() { 
    return build_duration; 
  }
  uint64_t getMaxError() override { return 256; }
  bool isErrorPageAligned() override { return false; }

private:
  uint64_t (*lookup)(uint64_t key, size_t *err);
  uint64_t size_in_bytes;
  uint64_t build_duration;
  KeyToPointConverter<T> *converter_;
};

template <class T> class RmiIndexBuilder : public IndexBuilder<T> {
public:
  RmiIndexBuilder(KeyToPointConverter<T> *converter):
      converter_(converter) {}
  void add(const T &t) override {}
  Index<KVSlice> *build() override { return new RmiIndex<T>(converter_); }
  
  void backToFile(std::string filename) override {}

private:
  KeyToPointConverter<T> *converter_;
};
} // namespace li_merge

#endif
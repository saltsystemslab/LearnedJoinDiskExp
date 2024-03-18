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

#include "fb_200M_uint64_0.h"
#include "osm_cellids_800M_uint64_0.h"
#include "books_800M_uint64_0.h"

namespace li_merge {

template <class T> class RmiIndex : public Index<T> {
public:
  RmiIndex(std::string dataset, KeyToPointConverter<T> *converter) : converter_(converter) {
    if (dataset=="fb_200M_uint64") {
        fb_200M_uint64_0::load("/home/chesetti/Repos/sosd/rmi_data");
        lookup = fb_200M_uint64_0::lookup;
    } else if (dataset=="osm_cellids_800M_uint64") {
        osm_cellids_800M_uint64_0::load("/home/chesetti/Repos/sosd/rmi_data");
        lookup = osm_cellids_800M_uint64_0::lookup;
    } else if (dataset=="books_800M_uint64") {
        books_800M_uint64_0::load("/home/chesetti/Repos/sosd/rmi_data");
        lookup = books_800M_uint64_0::lookup;
    } else {
        abort();
    }
  }
  Bounds getPositionBoundsRA(const T &t) override {
    uint64_t err;
    uint64_t pos = lookup(converter_->toPoint(t), &err); 
    return Bounds{pos-err, pos+err, 0};
  }
  Bounds getPositionBounds(const T &t) override { 
    uint64_t err;
    uint64_t pos = lookup(converter_->toPoint(t), &err); 
    return Bounds{pos-err, pos+err, 0};
  }
  uint64_t sizeInBytes() override { return fb_200M_uint64_0::RMI_SIZE; }
  uint64_t getMaxError() override { return 256; }
  bool isErrorPageAligned() override { return false; }

private:
  uint64_t (*lookup)(uint64_t key, size_t *err);
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
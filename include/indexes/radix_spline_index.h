#ifndef LEARNEDINDEXMERGE_RS_INDEX_H
#define LEARNEDINDEXMERGE_RS_INDEX_H

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

namespace li_merge {
template <class T> class RadixSplineIndex : public Index<T> {
public:
  RadixSplineIndex(rs::Builder<uint64_t> *rsb,
                   KeyToPointConverter<T> *converter, uint64_t max_error)
      : rs(rsb->Finalize()), converter_(converter), max_error_(max_error) {}

  RadixSplineIndex(std::string filename, KeyToPointConverter<T> *converter, uint64_t max_error)
      : converter_(converter), max_error_(max_error) {
    // Read the bytes on file and deserialize the radix spline index.
    std::ifstream file(filename, std::ios_base::binary);
    file.seekg(0, std::ios_base::end);
    int size = file.tellg();
    char *rsBytes = new char[size];
    file.seekg(0, std::ios_base::beg);
    file.read(rsBytes, size);
    file.close();
    std::string rsString(rsBytes, size);
    rs = rs::Serializer<uint64_t>::FromBytes(rsString);
    delete rsBytes;
  }

  Bounds getPositionBoundsRA(const T &t) override {
    rs::SearchBound bound = rs.GetSearchBound(converter_->toPoint(t));
    return Bounds{bound.begin, bound.end, 0};
  }
  Bounds getPositionBounds(const T &t) override {
    rs::SearchBound bound = rs.GetSearchBound(converter_->toPoint(t));
    return Bounds{bound.begin, bound.end, 0};
  }
  uint64_t sizeInBytes() override { return rs.GetSize(); }
  uint64_t getMaxError() override { return max_error_; }
  bool isErrorPageAligned() override { return false; }

private:
  rs::RadixSpline<uint64_t> rs;
  KeyToPointConverter<T> *converter_;
  uint64_t max_error_;
};

template <class T> class RadixSplineIndexBuilder : public IndexBuilder<T> {
public:
  RadixSplineIndexBuilder(KeyToPointConverter<T> *converter, uint64_t error)
      : converter_(converter), error_(error) {}

  void add(const T &t) override { keys_.push_back(converter_->toPoint(t)); }
  Index<KVSlice> *build() override {
    rsb = new rs::Builder<uint64_t>(keys_[0], keys_[keys_.size() - 1],
                                    radix_bits, error_);
    for (auto k : keys_) {
      rsb->AddKey(k);
    }
    return new RadixSplineIndex<KVSlice>(rsb, converter_, error_);
  }

  void backToFile(std::string filename) override {
    // We need to build again as calling finalize() again on the builder 
    // is not allowed.
    auto rsb2 = new rs::Builder<uint64_t>(keys_[0], keys_[keys_.size() - 1],
                                          radix_bits, error_);
    for (auto k : keys_) {
      rsb2->AddKey(k);
    }
    rs::RadixSpline<uint64_t> rs = rsb2->Finalize();
    std::string serializedRs;
    rs::Serializer<uint64_t>::ToBytes(rs, &serializedRs);
    std::ofstream file;
    file.open(filename, std::ios_base::binary);
    file.write(serializedRs.c_str(), serializedRs.size());
    file.close();
    keys_.clear();
    keys_.shrink_to_fit();
  }

private:
  const uint64_t radix_bits = 28; 
  // Using the hyperparameters from the SOSD dataset.
  uint64_t error_;
  std::vector<uint64_t> keys_;
  KeyToPointConverter<T> *converter_;
  rs::Builder<uint64_t> *rsb;
};
} // namespace li_merge

#endif
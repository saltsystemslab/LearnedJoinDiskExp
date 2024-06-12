#ifndef LEARNEDINDEXMERGE_PGM_INDEX_H
#define LEARNEDINDEXMERGE_PGM_INDEX_H

#include "config.h"
#include "key_to_point.h"
#include "key_value_slice.h"
#include "pgm/pgm_index.hpp"
#include "pgm/pgm_index_variants.hpp"
#include <algorithm>
#include <vector>

namespace li_merge {
template <class T, uint64_t Epsilon, uint64_t SampleFreq>
class PgmIndex : public Index<T> {
public:
  PgmIndex(pgm::PGMIndex<POINT_FLOAT_TYPE, Epsilon> *pgm_index,
           KeyToPointConverter<T> *converter)
      : pgm_index_(pgm_index), converter_(converter) {}

  PgmIndex(std::string filename, KeyToPointConverter<T> *converter)
      : converter_(converter)  {
    pgm_index_ = new pgm::MappedPGMIndex<POINT_FLOAT_TYPE, Epsilon>(filename);
  }

  Bounds getPositionBoundsRA(const T &t) override {
    auto bounds = pgm_index_->search(converter_->toPoint(t));
    return Bounds{bounds.lo * SampleFreq, (bounds.hi + 1) * SampleFreq,
                  bounds.pos * SampleFreq};
  }
  Bounds getPositionBounds(const T &t) override {
    auto bounds = pgm_index_->search(converter_->toPoint(t));
    return Bounds{bounds.lo * SampleFreq, (bounds.hi + 1) * SampleFreq,
                  bounds.pos * SampleFreq};
  }
  uint64_t sizeInBytes() override { return pgm_index_->size_in_bytes(); }
  uint64_t getMaxError() override {
    return (2 * pgm_index_->epsilon_value + 1) * SampleFreq;
  }
  bool isErrorPageAligned() override { return false; }

  Index<T> *shallow_copy() override { 
    return new PgmIndex<T, Epsilon, SampleFreq>(pgm_index_, converter_); 
  };
  

private:
  pgm::PGMIndex<POINT_FLOAT_TYPE, Epsilon> *pgm_index_;
  KeyToPointConverter<T> *converter_;
};

template <class T, uint64_t Epsilon, uint64_t SampleFreq>
class PgmIndexBuilder : public IndexBuilder<T> {
public:
  PgmIndexBuilder(KeyToPointConverter<T> *converter)
      : converter_(converter),
        sample_cd_(SampleFreq) {}

  void add(const T &t) override {
    sample_cd_--;
    if (sample_cd_ == 0) {
      sample_cd_ = SampleFreq;
      x_points_.push_back(converter_->toPoint(t));
    }
  }
  Index<KVSlice> *build() override {
    return new PgmIndex<T, Epsilon, SampleFreq>(
        new pgm::PGMIndex<POINT_FLOAT_TYPE, Epsilon>(x_points_), converter_);
  }

  void backToFile(std::string filename) override {
    auto index = new pgm::MappedPGMIndex<POINT_FLOAT_TYPE, Epsilon>(
        x_points_.begin(), x_points_.end(), filename);
    x_points_.clear();
    x_points_.shrink_to_fit();
    delete index;
  }

private:
  std::vector<POINT_FLOAT_TYPE> x_points_;
  KeyToPointConverter<T> *converter_;
  uint64_t sample_cd_;
};
} // namespace li_merge

#endif

#ifndef LEARNEDINDEXMERGE_ONE_LEVEL_PGM_INDEX_H
#define LEARNEDINDEXMERGE_ONE_LEVEL_PGM_INDEX_H

#include "config.h"
#include "key_to_point.h"
#include "key_value_slice.h"
#include "pgm/pgm_index.hpp"
#include "pgm/pgm_index_variants.hpp"
#include <algorithm>
#include <vector>

namespace li_merge {
template <class T, size_t Epsilon> class OneLevelPgmIndex : public Index<T> {
public:
  OneLevelPgmIndex(pgm::OneLevelPGMIndex<POINT_FLOAT_TYPE, Epsilon> *pgm_index,
                   KeyToPointConverter<T> *converter)
      : pgm_index_(pgm_index), converter_(converter), cur_segment_index_(0) {
    segments_ = new std::vector(pgm_index_->segments);
  }
  uint64_t getApproxPosition(const T &t) override {
    return pgm_index_->search(converter_->toPoint(t)).pos;
  }
  Bounds getPositionBounds(const T &t) override {
    auto bounds = pgm_index_->search(converter_->toPoint(t));
    return Bounds{bounds.lo, bounds.hi, bounds.pos};
  }
  uint64_t getApproxPositionMonotoneAccess(const T &t) override {
    POINT_FLOAT_TYPE point = converter_->toPoint(t);
    while (point >= (*segments_)[cur_segment_index_].key) {
      cur_segment_index_++;
    }
    if (cur_segment_index_)
      cur_segment_index_--;
    return std::min<size_t>((*segments_)[cur_segment_index_](point),
                            (*segments_)[cur_segment_index_ + 1].intercept);
  };
  Bounds getPositionBoundsMonotoneAccess(const T &t) override {
    return getPositionBounds(t);
  };
  void resetMonotoneAccess() override { cur_segment_index_ = 0; };

  uint64_t size_in_bytes() override {
    return segments_->size() * sizeof(segments_[0]);
  }

private:
  uint64_t cur_segment_index_;
  std::vector<typename pgm::PGMIndex<POINT_FLOAT_TYPE, Epsilon, 0, float>::Segment> *segments_;
  pgm::OneLevelPGMIndex<POINT_FLOAT_TYPE, Epsilon> *pgm_index_;
  KeyToPointConverter<T> *converter_;
};

template <class T, uint64_t Epsilon>
class OneLevelPgmIndexBuilder : public IndexBuilder<T> {
public:
  OneLevelPgmIndexBuilder(uint64_t approx_num_keys,
                          KeyToPointConverter<T> *converter)
      : converter_(converter) {
    x_points_.reserve(approx_num_keys);
  }
  void add(const T &t) override {
    double point = converter_->toPoint(t);
    x_points_.push_back(converter_->toPoint(t));
  }
  Index<KVSlice> *build() override {
    return new OneLevelPgmIndex<T, Epsilon>(
        new pgm::OneLevelPGMIndex<POINT_FLOAT_TYPE, Epsilon>(x_points_),
        converter_);
  }

private:
  std::vector<POINT_FLOAT_TYPE> x_points_;
  KeyToPointConverter<T> *converter_;
};
} // namespace li_merge

#endif
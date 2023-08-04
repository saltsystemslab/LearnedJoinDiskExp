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
                   KeyToPointConverter<T> *converter, uint64_t start_idx=0, uint64_t end_idx=-1)
      : pgm_index_(pgm_index), converter_(converter), cur_segment_index_(0), 
      start_idx_(start_idx), end_idx_(end_idx) {
    segments_ = new std::vector(pgm_index_->segments);
  }
  uint64_t getApproxPosition(const T &t) override {
    uint64_t pos = pgm_index_->search(converter_->toPoint(t)).pos;
    pos = std::clamp(pos, start_idx_, end_idx_);
    return pos - start_idx_;
  }
  Bounds getPositionBounds(const T &t) override {
    auto bounds = pgm_index_->search(converter_->toPoint(t));
    bounds.lo = std::clamp(bounds.lo, start_idx_, end_idx_);
    bounds.hi = std::clamp(bounds.lo, start_idx_, end_idx_);
    bounds.pos = std::clamp(bounds.lo, start_idx_, end_idx_);
    return Bounds{bounds.lo - start_idx_, bounds.hi-start_idx_, bounds.pos-start_idx_};
  }
  uint64_t getApproxPositionMonotoneAccess(const T &t) override {
    POINT_FLOAT_TYPE point = converter_->toPoint(t);
    while (point >= (*segments_)[cur_segment_index_].key) {
      cur_segment_index_++;
    }
    if (cur_segment_index_)
      cur_segment_index_--;
    uint64_t pos = std::min<size_t>((*segments_)[cur_segment_index_](point),
                            (*segments_)[cur_segment_index_ + 1].intercept);
    pos = std::clamp(pos, start_idx_, end_idx_);
    return pos - start_idx_;
  };
  uint64_t getApproxLowerBoundPosition(const T &t) override {
    uint64_t position = getApproxPosition(t);
    if (position >= Epsilon) {
      position = position - Epsilon;
    } else {
      position = 0;
    }
    position = std::clamp(position, start_idx_, end_idx_);
    return position - start_idx_;
  };
  Bounds getPositionBoundsMonotoneAccess(const T &t) override {
    return getPositionBounds(t);
  };
  uint64_t getApproxLowerBoundPositionMonotoneAccess(const T &t) override {
    uint64_t position = getApproxPositionMonotoneAccess(t);
    if (position >= Epsilon) {
      position = position - Epsilon;
    } else {
      position = 0;
    }
    position = std::clamp(position, start_idx_, end_idx_);
    return position - start_idx_;
  };
  void resetMonotoneAccess() override { cur_segment_index_ = 0; };

  uint64_t size_in_bytes() override {
    return segments_->size() * sizeof(segments_[0]);
  }
  Index<T> *getIndexForSubrange(uint64_t start, uint64_t end) override {
    return new OneLevelPgmIndex(pgm_index_, converter_, start, end);
  }

private:
  uint64_t start_idx_;
  uint64_t end_idx_;
  uint64_t cur_segment_index_;
  std::vector<typename pgm::PGMIndex<POINT_FLOAT_TYPE, Epsilon, 0,
                                     float>::Segment> *segments_;
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
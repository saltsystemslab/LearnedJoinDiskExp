#ifndef LEARNEDINDEXMERGE_PGM_INDEX_H
#define LEARNEDINDEXMERGE_PGM_INDEX_H

#include "config.h"
#include "key_to_point.h"
#include "key_value_slice.h"
#include "pgm/pgm_index.hpp"
#include <algorithm>
#include <vector>

namespace li_merge {
template <class T, uint64_t Epsilon> class PgmIndex : public Index<T> {
public:
  PgmIndex(pgm::PGMIndex<POINT_FLOAT_TYPE, Epsilon> *pgm_index,
           KeyToPointConverter<T> *converter, uint64_t start_idx = 0,
           uint64_t end_idx = -1)
      : pgm_index_(pgm_index), converter_(converter), start_idx_(start_idx),
        end_idx_(end_idx) {}
  Bounds getPositionBounds(const T &t) override {
    auto bounds = pgm_index_->search(converter_->toPoint(t));
    bounds.lo = std::clamp(bounds.lo, start_idx_, end_idx_);
    bounds.hi = std::clamp(bounds.hi, start_idx_, end_idx_);
    bounds.pos = std::clamp(bounds.pos, start_idx_, end_idx_);
    return Bounds{bounds.lo - start_idx_, bounds.hi - start_idx_,
                  bounds.pos - start_idx_};
  }
  uint64_t sizeInBytes() override { return pgm_index_->size_in_bytes(); }
  uint64_t getMaxError() override { return 2 * pgm_index_->epsilon_value + 1; }
  Index<T> *getIndexForSubrange(uint64_t start, uint64_t end) override {
    return new PgmIndex(pgm_index_, converter_, start, end);
  }
  bool isErrorPageAligned() override { return false; }

private:
  uint64_t start_idx_;
  uint64_t end_idx_;
  pgm::PGMIndex<POINT_FLOAT_TYPE, Epsilon> *pgm_index_;
  KeyToPointConverter<T> *converter_;
};

template <class T, uint64_t Epsilon>
class PgmIndexBuilder : public IndexBuilder<T> {
public:
  PgmIndexBuilder(uint64_t approx_num_keys, KeyToPointConverter<T> *converter)
      : converter_(converter) {
    x_points_.reserve(approx_num_keys);
  }
  void add(const T &t) override { x_points_.push_back(converter_->toPoint(t)); }
  Index<KVSlice> *build() override {
    return new PgmIndex<T, Epsilon>(
        new pgm::PGMIndex<POINT_FLOAT_TYPE, Epsilon>(x_points_), converter_);
  }

private:
  std::vector<POINT_FLOAT_TYPE> x_points_;
  KeyToPointConverter<T> *converter_;
};
} // namespace li_merge

#endif

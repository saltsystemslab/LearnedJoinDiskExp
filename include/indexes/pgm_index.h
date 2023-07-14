#ifndef LEARNEDINDEXMERGE_PGM_INDEX_H
#define LEARNEDINDEXMERGE_PGM_INDEX_H

#include "config.h"
#include "key_to_point.h"
#include "key_value_slice.h"
#include "pgm/pgm_index.hpp"
#include <vector>

namespace li_merge {
template <class T> class PgmIndex : public Index<T> {
public:
  PgmIndex(pgm::PGMIndex<POINT_FLOAT_TYPE, 64> *pgm_index,
           KeyToPointConverter<T> *converter)
      : pgm_index_(pgm_index), converter_(converter) {}
  uint64_t getApproxPosition(const T &t) override {
    return pgm_index_->search(converter_->toPoint(t)).pos;
  }
  Bounds getPositionBounds(const T &t) override {
    auto bounds = pgm_index_->search(converter_->toPoint(t));
    return Bounds{bounds.lo, bounds.hi, bounds.pos};
  }

private:
  pgm::PGMIndex<POINT_FLOAT_TYPE, 64> *pgm_index_;
  KeyToPointConverter<T> *converter_;
};

template <class T> class PgmIndexBuilder : public IndexBuilder<T> {
public:
  PgmIndexBuilder(uint64_t approx_num_keys, KeyToPointConverter<T> *converter)
      : converter_(converter) {
    x_points_.reserve(approx_num_keys);
  }
  void add(const T &t) override { x_points_.push_back(converter_->toPoint(t)); }
  Index<KVSlice> *build() override {
    return new PgmIndex<T>(new pgm::PGMIndex<POINT_FLOAT_TYPE, 64>(x_points_),
                           converter_);
  }

private:
  std::vector<POINT_FLOAT_TYPE> x_points_;
  KeyToPointConverter<T> *converter_;
};
} // namespace li_merge

#endif
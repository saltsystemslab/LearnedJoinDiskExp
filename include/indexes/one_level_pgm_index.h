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
      : pgm_index_(pgm_index), converter_(converter), cur_segment_index_(0) {}
  OneLevelPgmIndex(std::string filename, KeyToPointConverter<T> *converter)
      : converter_(converter) {
    fprintf(stderr, "%s\n", filename.c_str());
    pgm_index_ =
        new pgm::MappedPGMIndex<POINT_FLOAT_TYPE, Epsilon, 0>(filename);
  }
  Bounds getPositionBoundsRA(const T &t) override {
    auto bounds = pgm_index_->search(converter_->toPoint(t));
    return Bounds{bounds.lo, bounds.hi + 1, bounds.pos};
  }
  Bounds getPositionBounds(const T &t) override {
    auto bounds =
        pgm_index_->linearSearch(converter_->toPoint(t), &cur_segment_index_);
    return Bounds{bounds.lo, bounds.hi + 1, bounds.pos};
  }
  uint64_t sizeInBytes() override { return pgm_index_->size_in_bytes(); }
  uint64_t getMaxError() override { return 2 * pgm_index_->epsilon_value + 1; }
  bool isErrorPageAligned() override { return false; }
  Index<T> *shallow_copy() override {
    return new OneLevelPgmIndex(pgm_index_, converter_);
  }

private:
  uint64_t cur_segment_index_;
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
  OneLevelPgmIndexBuilder(uint64_t approx_num_keys,
                          KeyToPointConverter<T> *converter,
                          std::string filename)
      : converter_(converter), filename_(filename) {
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
  // TODO: Overrride this and make take a string.
  void backToFile() {
    new OneLevelPgmIndex<T, Epsilon>(
        new pgm::MappedPGMIndex<POINT_FLOAT_TYPE, Epsilon, 0>(
            x_points_.begin(), x_points_.end(), filename_),
        converter_);
  }

private:
  std::vector<POINT_FLOAT_TYPE> x_points_;
  KeyToPointConverter<T> *converter_;
  std::string filename_;
};
} // namespace li_merge

#endif

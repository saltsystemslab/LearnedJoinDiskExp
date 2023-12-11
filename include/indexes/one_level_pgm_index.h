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
  OneLevelPgmIndex(
    pgm::OneLevelPGMIndex<POINT_FLOAT_TYPE, Epsilon> *pgm_index,
    KeyToPointConverter<T> *converter,
    uint64_t sample_freq
  ) : 
  pgm_index_(pgm_index), 
  converter_(converter), 
  cur_segment_index_(0),
  sample_freq_(sample_freq) {}

  OneLevelPgmIndex(
    std::string filename, 
    KeyToPointConverter<T> *converter,
    uint64_t sample_freq): 
    converter_(converter), 
    cur_segment_index_(0),
    sample_freq_(sample_freq) {
    fprintf(stderr, "%s\n", filename.c_str());
    pgm_index_ =
        new pgm::MappedPGMIndex<POINT_FLOAT_TYPE, Epsilon, 0>(filename);
  }
  Bounds getPositionBoundsRA(const T &t) override {
    auto bounds = pgm_index_->search(converter_->toPoint(t));
    // fprintf(stderr, "[%ld %ld]\n", bounds.lo * sample_freq_, (bounds.hi + 1) * sample_freq_);
    return Bounds{
      bounds.lo * sample_freq_, 
      (bounds.hi + 1) * sample_freq_, 
      bounds.pos * sample_freq_
    };
  }
  Bounds getPositionBounds(const T &t) override {
    auto bounds =
        pgm_index_->linearSearch(converter_->toPoint(t), &cur_segment_index_);
    return Bounds{
      bounds.lo * sample_freq_, 
      (bounds.hi + 1) * sample_freq_, 
      bounds.pos * sample_freq_
    };
  }
  uint64_t sizeInBytes() override { return pgm_index_->size_in_bytes(); }
  uint64_t getMaxError() override { return (2 * pgm_index_->epsilon_value + 1) * sample_freq_; }
  bool isErrorPageAligned() override { return false; }
  Index<T> *shallow_copy() override {
    return new OneLevelPgmIndex(pgm_index_, converter_, sample_freq_);
  }

private:
  uint64_t sample_freq_;
  uint64_t cur_segment_index_;
  pgm::OneLevelPGMIndex<POINT_FLOAT_TYPE, Epsilon> *pgm_index_;
  KeyToPointConverter<T> *converter_;
};

template <class T, uint64_t Epsilon>
class OneLevelPgmIndexBuilder : public IndexBuilder<T> {
public:
  OneLevelPgmIndexBuilder(uint64_t approx_num_keys, uint64_t sample_freq,
                          KeyToPointConverter<T> *converter)
      : converter_(converter), sample_freq_(sample_freq), sample_cd_(sample_freq) {
    x_points_.reserve(approx_num_keys);
  }
  OneLevelPgmIndexBuilder(uint64_t approx_num_keys, uint64_t sample_freq,
                          KeyToPointConverter<T> *converter,
                          std::string filename)
      : converter_(converter), filename_(filename), 
      sample_freq_(sample_freq), sample_cd_(sample_freq) {
    x_points_.reserve(approx_num_keys);
  }
  void add(const T &t) override {
    double point = converter_->toPoint(t);
    sample_cd_--;
    if (!sample_cd_) {
      sample_cd_ = sample_freq_;
      x_points_.push_back(converter_->toPoint(t));
    }
  }
  Index<KVSlice> *build() override {
    return new OneLevelPgmIndex<T, Epsilon>(
        new pgm::OneLevelPGMIndex<POINT_FLOAT_TYPE, Epsilon>(x_points_),
        converter_, sample_freq_);
  }
  // TODO: Overrride this and make take a string.
  void backToFile() override {
        new pgm::MappedPGMIndex<POINT_FLOAT_TYPE, Epsilon, 0>(
            x_points_.begin(), x_points_.end(), filename_);
    x_points_.clear();
    delete index;
    vector<POINT_FLOAT_TYPE>().swap(x_points_);
  }

private:
  std::vector<POINT_FLOAT_TYPE> x_points_;
  KeyToPointConverter<T> *converter_;
  std::string filename_;
  uint64_t sample_freq_;
  uint64_t sample_cd_;
};
} // namespace li_merge

#endif

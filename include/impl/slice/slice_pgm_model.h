#ifndef SLICE_PGM_MODEL_H
#define SLICE_PGM_MODEL_H

#include "iterator.h"
#include "model.h"
#include "plr.h"
#include "slice.h"
#include "pgm/pgm_index.hpp"

#include <iostream>
using namespace std;


class SlicePGMModel : public Model<Slice> {
public:
  SlicePGMModel(pgm::PGMIndex<PLR_SEGMENT_POINT, 64> *pgm_index, uint64_t num_keys, 
    SliceToPlrPointConverter *slice_to_point_converter)
      : pgm_index_(pgm_index), num_keys_(num_keys),
        start_offset_(0), converter_(slice_to_point_converter) {
  }

  SlicePGMModel(pgm::PGMIndex<PLR_SEGMENT_POINT, 64> *pgm_index, uint64_t num_keys, uint64_t start_offset,
    SliceToPlrPointConverter *slice_to_point_converter)
      : pgm_index_(pgm_index), num_keys_(num_keys),
        start_offset_(start_offset), converter_(slice_to_point_converter) {
    }

  uint64_t guessPositionMonotone(Slice target_slice_key) override {
    PLR_SEGMENT_POINT point = converter_->to_plr_point(target_slice_key);
    auto pos = pgm_index_->search(point).pos;
    if (pos > start_offset_) {
      return pos - start_offset_;
    }
    return 0;
  };

  uint64_t guessPosition(Slice target_slice_key) override {
    PLR_SEGMENT_POINT point = converter_->to_plr_point(target_slice_key);
    auto pos = pgm_index_->search(point).pos;
    if (pos > start_offset_) {
      return pos - start_offset_;
    }
    return 0;
  }

  SlicePGMModel *get_model_for_subrange(uint64_t start, uint64_t end) override {
    return new SlicePGMModel(pgm_index_, end-start, start, converter_);
  }

  double getMaxError() override { return 64; }

  uint64_t size_bytes() override {
    return pgm_index_->size_in_bytes();
  }

private:
  pgm::PGMIndex<PLR_SEGMENT_POINT, 64> *pgm_index_;
  SliceToPlrPointConverter *converter_;
  uint64_t start_offset_;
  uint64_t num_keys_;
};

class PGMModelBuilder: public ModelBuilder<Slice> {
public:
  PGMModelBuilder(SliceToPlrPointConverter *converter):
    converter_(converter) {}

  void add(const Slice &slice_key) override {
    PLR_SEGMENT_POINT t = converter_->to_plr_point(slice_key);
    points.push_back(t);
  }
  SlicePGMModel *finish() override {
    return new SlicePGMModel(new pgm::PGMIndex(points), points.size(), converter_);
  }

private:
  vector<PLR_SEGMENT_POINT> points;
  SliceToPlrPointConverter *converter_;
};

#endif

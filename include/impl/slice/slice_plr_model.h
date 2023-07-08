#ifndef SLICE_PLR_MODEL_H
#define SLICE_PLR_MODEL_H

#include "iterator.h"
#include "model.h"
#include "plr.h"
#include "slice.h"
#include <iostream>
using namespace std;

PLR_SEGMENT_POINT interpret_slice_to_number(const Slice &t) { return 0; }

class SliceToPlrPointConverter {
  public:
    virtual PLR_SEGMENT_POINT to_plr_point(const Slice &t) = 0;
};

class FixedKeySizeToPlrPointConverter : public SliceToPlrPointConverter {
public:
  PLR_SEGMENT_POINT to_plr_point(const Slice &t) {
    PLR_SEGMENT_POINT num = 0;
    for (int i = 0; i < t.size_; i++) {
      uint8_t c = *(t.data_ + i);
      num = num * 256 + c;
    }
    return num;
  }
};

class SliceAsUint64PlrPointConverter : public SliceToPlrPointConverter {
  public:
    PLR_SEGMENT_POINT to_plr_point(const Slice &t) {
      uint64_t *a = (uint64_t *)t.data_;
      return (PLR_SEGMENT_POINT)(*a);
    }
};

class SlicePLRModel : public Model<Slice> {
public:
  SlicePLRModel(PLRModel *model, uint64_t num_keys, SliceToPlrPointConverter *slice_to_point_converter)
      : model_(model), plr_segment_index_(0), num_keys_(num_keys),
        start_offset_(0), slice_to_point_converter_(slice_to_point_converter) {
#if PRINT_MODEL
    std::vector<Segment> &segments = model_->lineSegments_;
    printf("Model Start\n");
    for (auto s: segments) {
      printf("seg: %lf %lf %0.32lf %lf\n", s.x, s.last, s.k, s.b);
    }
    printf("Model End\n");
  #endif
  }

  SlicePLRModel(PLRModel *model, uint64_t start_offset, uint64_t num_keys, SliceToPlrPointConverter *slice_to_point_converter)
      : model_(model), start_offset_(start_offset), plr_segment_index_(0),
        num_keys_(num_keys), slice_to_point_converter_(slice_to_point_converter) {}

  uint64_t guessPositionMonotone(Slice target_slice_key) override {
    PLR_SEGMENT_POINT target_key = slice_to_point_converter_->to_plr_point(target_slice_key);
    std::vector<Segment> &segments = model_->lineSegments_;
    for (uint64_t i = getPLRLineSegmentIndex(); i < (uint64_t)segments.size();
         i++) {
      if (segments[i].last > target_key) {
        setPLRLineSegmentIndex(i);
        uint64_t result = std::ceil(target_key * segments[i].k + segments[i].b -
                                    start_offset_);
        if (result < 0) {
          result = 0;
	        abort();
        }
        if (result >= num_keys_) {
          result = num_keys_ - 1;
        }
        return result;
      }
    }
    return num_keys_ - 1;
  };

  uint64_t guessPosition(Slice target_slice_key) override {
    PLR_SEGMENT_POINT target_key = slice_to_point_converter_->to_plr_point(target_slice_key);
    std::vector<Segment> &segments = model_->lineSegments_;
    int64_t left = 0, right = (int64_t)segments.size() - 1;
    while (left < right) {
      int64_t mid = (right + left + 1) / 2;
      if (target_key < segments[mid].x)
        right = mid - 1;
      else
        left = mid;
    }
    uint64_t result = std::ceil((target_key)*segments[left].k +
                                segments[left].b - start_offset_);
    if (result < 0) {
      result = 0;
    }
    std::cout<<"target_key: "<<target_key<<"approx_pos: " <<result<<" "<<num_keys_<<std::endl;
    if (result >= num_keys_) {
      result = num_keys_ - 1;
      std::cout<<target_key<<std::endl;
      std::cout<<segments[left].x<<" "<<segments[left].k<<" "<<segments[left].b<<std::endl;
      abort();
    }
    return result;
  }

  SlicePLRModel *get_model_for_subrange(uint64_t start, uint64_t end) override {
    return new SlicePLRModel(model_, start, end - start, slice_to_point_converter_);
  }

  double getMaxError() override { return model_->gamma_; }

  uint64_t size_bytes() override {
    return model_->lineSegments_.size() * sizeof(Segment);
  }

private:
  PLRModel *model_;
  SliceToPlrPointConverter *slice_to_point_converter_;
  uint64_t plr_segment_index_;
  uint64_t start_offset_;
  uint64_t num_keys_;
  uint64_t getPLRLineSegmentIndex() { return plr_segment_index_; };
  void setPLRLineSegmentIndex(uint64_t value) { plr_segment_index_ = value; }
};

static double last_key = 0.0;
class SlicePLRModelBuilder : public ModelBuilder<Slice> {
public:
  SlicePLRModelBuilder(double gamma, SliceToPlrPointConverter *converter)
      : plrBuilder_(new PLRBuilder(gamma)), num_keys_(0), converter_(converter) {}
  void add(const Slice &slice_key) override {
    PLR_SEGMENT_POINT t = converter_->to_plr_point(slice_key);
    if (t < last_key) {
      printf("plr_points not sorted!");
      abort();
    }
    last_key = t;
    num_keys_++;
    plrBuilder_->processKey(t);
  }
  SlicePLRModel *finish() override {
    printf("cdf end");
    last_key = 0;
    return new SlicePLRModel(plrBuilder_->finishTraining(), num_keys_, converter_);
  }

private:
  SliceToPlrPointConverter *converter_;
  PLRBuilder *plrBuilder_;
  uint64_t num_keys_;
};

#endif

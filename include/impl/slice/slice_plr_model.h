#ifndef SLICE_PLR_MODEL_H
#define SLICE_PLR_MODEL_H

#include "iterator.h"
#include "model.h"
#include "plr.h"
#include "slice.h"

PLR_SEGMENT_POINT interpret_slice_to_number(const Slice &t) { return 0; }

class SlicePLRModel : public Model<Slice> {
public:
  SlicePLRModel(PLRModel *model, uint64_t num_keys)
      : model_(model), plr_segment_index_(0), num_keys_(num_keys),
        start_offset_(0) {}

  SlicePLRModel(PLRModel *model, uint64_t start_offset, uint64_t num_keys)
      : model_(model), start_offset_(start_offset), plr_segment_index_(0),
        num_keys_(num_keys) {}

  uint64_t guessPositionMonotone(Slice target_slice_key) override {
    PLR_SEGMENT_POINT target_key = target_slice_key.to_double();
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
    PLR_SEGMENT_POINT target_key = target_slice_key.to_double();
    std::vector<Segment> &segments = model_->lineSegments_;
    int32_t left = 0, right = (int32_t)segments.size() - 1;
    while (left < right) {
      int32_t mid = (right + left + 1) / 2;
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
    if (result >= num_keys_) {
      result = num_keys_ - 1;
    }
    return result;
  }

  SlicePLRModel *get_model_for_subrange(uint64_t start, uint64_t end) override {
    return new SlicePLRModel(model_, start, end - start);
  }

  double getMaxError() override { return model_->gamma_; }

  uint64_t size_bytes() override {
    return model_->lineSegments_.size() * sizeof(Segment);
  }

private:
  PLRModel *model_;
  uint64_t plr_segment_index_;
  uint64_t start_offset_;
  uint64_t num_keys_;
  uint64_t getPLRLineSegmentIndex() { return plr_segment_index_; };
  void setPLRLineSegmentIndex(uint64_t value) { plr_segment_index_ = value; }
};

class SlicePLRModelBuilder : public ModelBuilder<Slice> {
public:
  SlicePLRModelBuilder(double gamma)
      : plrBuilder_(new PLRBuilder(gamma)), num_keys_(0) {}
  void add(const Slice &slice_key) override {
    PLR_SEGMENT_POINT t = slice_key.to_double();
    num_keys_++;
    plrBuilder_->processKey(t);
  }
  SlicePLRModel *finish() override {
    return new SlicePLRModel(plrBuilder_->finishTraining(), num_keys_);
  }

private:
  PLRBuilder *plrBuilder_;
  uint64_t num_keys_;
};

#endif

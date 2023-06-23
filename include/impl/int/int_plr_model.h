#ifndef INT_PLR_MODEL_H
#define INT_PLR_MODEL_H

#include "iterator.h"
#include "model.h"
#include "plr.h"

template <class T> class IntPLRModel : public Model<T> {
public:
  IntPLRModel(PLRModel *model, uint64_t num_keys)
      : model_(model), plr_segment_index_(0), num_keys_(num_keys),
        start_offset_(0) {}

  IntPLRModel(PLRModel *model, uint64_t start_offset, uint64_t num_keys)
      : model_(model), start_offset_(start_offset), plr_segment_index_(0),
        num_keys_(num_keys) {}

  uint64_t guessPositionMonotone(T target_key) override {
    std::vector<Segment> &segments = model_->lineSegments_;
    for (uint64_t i = getPLRLineSegmentIndex(); i < (uint64_t)segments.size();
         i++) {
      if (segments[i].last > target_key) {
        setPLRLineSegmentIndex(i);
        uint64_t result = std::ceil(target_key * segments[i].k + segments[i].b -
                                    start_offset_);
        if (result < 0) {
          result = 0;
        }
        if (result >= num_keys_) {
          result = num_keys_ - 1;
        }
        return result;
      }
    }
    return num_keys_ - 1;
  };

  uint64_t guessPosition(T target_key) override {
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

  Model<T> *get_model_for_subrange(uint64_t start, uint64_t end) override {
    return new IntPLRModel(model_, start, end - start);
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

template <class T> class IntPLRModelBuilder : public ModelBuilder<T> {
public:
  IntPLRModelBuilder(double gamma)
      : plrBuilder_(new PLRBuilder(gamma)), num_keys_(0) {}
  void add(const T &t) override {
    num_keys_++;
    plrBuilder_->processKey(t);
  }
  IntPLRModel<T> *finish() override {
    return new IntPLRModel<T>(plrBuilder_->finishTraining(), num_keys_);
  }

private:
  PLRBuilder *plrBuilder_;
  uint64_t num_keys_;
};

#endif

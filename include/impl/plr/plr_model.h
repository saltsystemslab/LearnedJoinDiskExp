#ifndef PLR_MODEL_H
#define PLR_MODEL_H

#include "iterator.h"
#include "model.h"
#include "plr.h"
#include "slice.h"

template <class T>
class PLR_Model : public Model<T> {
 public:
  PLR_Model(PLRModel *model, uint64_t num_keys)
      : model_(model), plr_segment_index_(0), num_keys_(num_keys), start_offset_(0) {}

  PLR_Model(PLRModel *model, uint64_t start_offset, uint64_t num_keys)
      : model_(model), start_offset_(start_offset), plr_segment_index_(0), num_keys_(num_keys) {}

  uint64_t guessPositionMonotone(T target_key) override {
    std::vector<Segment> &segments = model_->lineSegments_;
    for (uint64_t i = getPLRLineSegmentIndex(); i < (uint64_t)segments.size();
         i++) {
      if (segments[i].last > target_key) {
        setPLRLineSegmentIndex(i);
        uint64_t result = std::ceil(target_key * segments[i].k + segments[i].b - start_offset_);
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

    uint64_t result = std::ceil((target_key) * segments[left].k + segments[left].b - start_offset_);
    if (result < 0) {
      result = 0;
    }
    if (result >= num_keys_) {
      result = num_keys_ - 1;
    }
    return result;
  }

  Model<T> *get_model_for_subrange(const T& start, const T& end) override {
    return new PLR_Model(model_, start, end-start); 
  }

 private:
  PLRModel *model_;
  uint64_t plr_segment_index_;
  uint64_t start_offset_;
  uint64_t num_keys_;
  uint64_t getPLRLineSegmentIndex() { return plr_segment_index_; };
  void setPLRLineSegmentIndex(uint64_t value) { plr_segment_index_ = value; }
};

template <class T>
class PLRModelBuilder : public ModelBuilder<T> {
 public:
  PLRModelBuilder() : plrBuilder_(new PLRBuilder(10)), num_keys_(0) {}
  void add(const T &t) override {
    num_keys_++;
    plrBuilder_->processKey(t);
  }
  PLR_Model<T> *finish() override {
    return new PLR_Model<T>(plrBuilder_->finishTraining(), num_keys_);
  }
 private:
  PLRBuilder *plrBuilder_;
  uint64_t num_keys_;
};

#endif

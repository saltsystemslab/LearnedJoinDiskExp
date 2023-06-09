#ifndef PLR_MODEL_H
#define PLR_MODEL_H

#include "plr.h"
#include "slice.h"
#include "model.h"
#include "iterator.h"

class PLR_Model : public Model<Slice> {
public:
  PLR_Model(PLRModel *model, uint64_t num_keys):
  model_(model),
  plr_segment_index_(0),
  num_keys_(num_keys)
  {}
  double guessPositionMonotone(Slice target_key) override;
  double guessPositionUsingBinarySearch(Slice target_key) override;
private:
  PLRModel *model_;
  uint64_t plr_segment_index_;
  uint64_t num_keys_;
  uint64_t getPLRLineSegmentIndex();
  void setPLRLineSegmentIndex(uint64_t value);
};

class PLRModelBuilder : public ModelBuilder<Slice> {
public:
  PLRModelBuilder()
  : plrBuilder_(new PLRBuilder(10)), num_keys_(0) {}
  void add(const Slice &t) override {
    num_keys_++;
    #if USE_STRING_KEYS
    plrBuilder_->processKey(LdbKeyToInteger(t));
#else
    plrBuilder_->processKey(*(KEY_TYPE *)(t.data_));
#endif
  }
  PLR_Model* finish() override {
    return new PLR_Model(plrBuilder_->finishTraining(), num_keys_);
  }

private:
  PLRBuilder *plrBuilder_;
  uint64_t num_keys_;
};


#endif

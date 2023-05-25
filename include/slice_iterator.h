#ifndef SLICE_ITERATOR_H
#define SLICE_ITERATOR_H

#include "iterator.h"
#include "plr.h"
#include "slice.h"

class SliceIterator : public Iterator<Slice> {
public:
  SliceIterator();
  double guessPosition(Slice target_key) override;
  int index() override { return index_; }

protected:
  PLRModel *model;
  uint64_t num_keys_;
  int index_;

private:
  uint64_t plr_segment_index;
  uint64_t getPLRLineSegmentIndex();
  void setPLRLineSegmentIndex(uint64_t value);
};
#endif

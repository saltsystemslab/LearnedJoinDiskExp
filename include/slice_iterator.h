#ifndef SLICE_ITERATOR_H
#define SLICE_ITERATOR_H

#include "iterator.h"
#include "plr.h"
#include "slice.h"

class SliceIteratorWithModel : public Iterator<Slice> {
public:
  SliceIteratorWithModel(Iterator<Slice> *iterator, PLRModel *model)
      : iterator_(iterator), model_(model), plr_segment_index_(0){};
  bool valid() const override { return iterator_->valid(); }
  void next() override { iterator_->next(); }
  Slice peek(uint64_t pos) const override { return iterator_->peek(pos); }
  void seek(Slice item) override { iterator_->seek(item); }
  void seekToFirst() override { iterator_->seekToFirst(); }
  Slice key() override { return iterator_->key(); }
  uint64_t current_pos() const override { return iterator_->current_pos(); }
  uint64_t num_keys() const { return iterator_->num_keys(); }
  double guessPositionMonotone(Slice target_key) override;
  uint64_t bulkReadAndForward(uint64_t num_keys, char **data, uint64_t *len) {
    return iterator_->bulkReadAndForward(num_keys, data, len);
  };

private:
  PLRModel *model_;
  Iterator<Slice> *iterator_;
  uint64_t plr_segment_index_;
  uint64_t getPLRLineSegmentIndex();
  void setPLRLineSegmentIndex(uint64_t value);
};

#endif

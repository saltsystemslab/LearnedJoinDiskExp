#ifndef SLICE_ARRAY_H
#define SLICE_ARRAY_H

#include "plr.h"
#include "slice.h"
#include "slice_iterator.h"

class SliceArrayIterator : public SliceIterator {
public:
  SliceArrayIterator(char *a, int n, int key_size, PLRModel *model, int index) {
    this->a = a;
    this->cur = 0;
    this->num_keys_ = n;
    this->key_size = key_size;
    this->model = model;
    this->index_ = index;
  }
  ~SliceArrayIterator();
  bool valid() const override;
  void next() override;
  Slice peek(uint64_t pos) const override;
  void seek(Slice item) override;
  void seekToFirst() override;
  Slice key() override;
  uint64_t current_pos() const override;

private:
  char *a;
  int key_size;
  int cur;
};

class SliceArrayBuilder : public IteratorBuilder<Slice> {
public:
  SliceArrayBuilder(int n, int key_size, int index);
  void add(const Slice &t) override;
  SliceArrayIterator *finish() override;

private:
  char *a;
  int n;
  int cur;
  int key_size;
  int index_;
#if LEARNED_MERGE
  PLRBuilder *plrBuilder;
#endif
};
#endif

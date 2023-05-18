#ifndef SLICE_ARRAY_H
#define SLICE_ARRAY_H

#include "iterator.h"

class SliceArrayIterator : public Iterator<Slice> {
public:
  SliceArrayIterator(char *a, int n, int key_size);
  ~SliceArrayIterator();
  bool valid() const override;
  void next() override;
  Slice peek(uint64_t pos) const override;
  void seek(Slice item) override;
  void seekToFirst() override;
  Slice key() const override;
  uint64_t current_pos() const override;

private:
  char *a;
  int key_size;
  int cur;
  int n;
};

class SliceArrayBuilder : public IteratorBuilder<Slice> {
public:
  SliceArrayBuilder(int n, int key_size);
  void add(Slice t) override;
  SliceArrayIterator *finish() override;

private:
  char *a;
  int n;
  int cur;
  int key_size;
};
#endif
#ifndef SLICE_ARRAY_H
#define SLICE_ARRAY_H

#include "plr.h"
#include "slice.h"
#include "slice_iterator.h"

class SliceArrayIterator : public Iterator<Slice> {
public:
  SliceArrayIterator(char *a, int n, int key_size, std::string id)
      : id_(id), num_keys_(n), key_size_(key_size), cur_(0), a_(a) {}
  ~SliceArrayIterator();
  bool valid() const override;
  void next() override;
  Slice peek(uint64_t pos) const override;
  void seek(Slice item) override;
  void seekToFirst() override;
  Slice key() override;
  uint64_t current_pos() const override;
  std::string identifier() override { return id_; }
  uint64_t num_keys() const override { return num_keys_; }

private:
  char *a_;
  int key_size_;
  uint64_t num_keys_;
  int cur_;
  std::string id_;
};

class SliceArrayBuilder : public IteratorBuilder<Slice> {
public:
  SliceArrayBuilder(int n, int key_size, int index);
  void add(const Slice &t) override;
  Iterator<Slice> *finish() override;

private:
  char *a;
  int n;
  int cur;
  int key_size;
  int index_;
};

class SliceArrayWithModelBuilder : public SliceArrayBuilder {
public:
  SliceArrayWithModelBuilder(int n, int key_size, int index)
      : SliceArrayBuilder(n, key_size, index),
        plrBuilder_(new PLRBuilder(PLR_ERROR_BOUND)) {}

  void add(const Slice &key) override {
    SliceArrayBuilder::add(key);
#if USE_STRING_KEYS
    plrBuilder_->processKey(LdbKeyToInteger(key));
#else
    plrBuilder_->processKey(*(KEY_TYPE *)(key.data_));
#endif
  };
  Iterator<Slice> *finish() override {
    return new SliceIteratorWithModel(SliceArrayBuilder::finish(),
                                      plrBuilder_->finishTraining());
  };

private:
  PLRBuilder *plrBuilder_;
};
#endif

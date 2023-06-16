#ifndef SLICE_ARRAY_H
#define SLICE_ARRAY_H

#include "iterator.h"
#include "plr.h"
#include "slice.h"

class SliceArrayIterator : public Iterator<Slice> {
 public:
  SliceArrayIterator(char *a, int n, int key_size, std::string id)
      : id_(id), num_keys_(n), key_size_(key_size), cur_(0), a_(a) {}
  ~SliceArrayIterator() { delete a_; }
  bool valid() const override { return cur_ < num_keys_; }
  void next() { cur_++; }
  Slice peek(uint64_t pos) const override {
    return Slice(a_ + pos * key_size_, key_size_);
  };
  void seekToFirst() override { cur_ = 0; };
  Slice key() override { return Slice(a_ + cur_ * key_size_, key_size_); }
  uint64_t current_pos() const override { return cur_; };
  std::string id() override { return id_; }
  uint64_t num_keys() const override { return num_keys_; }
  uint64_t bulkReadAndForward(uint64_t num_keys, char **data,
                              uint64_t *len) override {
    abort();
  }
  Iterator<Slice> *subRange(uint64_t start, uint64_t end) override { abort(); }

 private:
  char *a_;
  int key_size_;
  uint64_t num_keys_;
  int cur_;
  std::string id_;
};

class SliceArrayBuilder : public IteratorBuilder<Slice> {
 public:
  SliceArrayBuilder(uint64_t n, int key_size, std::string id)
      : a_(new char[n * key_size]),
        n_(n),
        key_size_(key_size),
        id_(id),
        cur_idx_(0){};
  void add(const Slice &t) override {
    memcpy(a_ + cur_idx_ * key_size_, t.data_, key_size_);
    cur_idx_++;
  };

  Iterator<Slice> *build() {
    return new SliceArrayIterator(a_, n_, key_size_, id_);
  };

  void bulkAdd(Iterator<Slice> *iter, uint64_t num_keys) override { abort(); };
  IteratorBuilder<Slice> *subRange(uint64_t start, uint64_t end) override {
    abort();
  };

 private:
  char *a_;
  uint64_t n_;
  uint64_t cur_idx_;
  int key_size_;
  std::string id_;
};

#endif

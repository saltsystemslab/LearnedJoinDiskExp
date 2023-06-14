#ifndef SLICE_ARRAY_H
#define SLICE_ARRAY_H

#include "plr.h"
#include "slice.h"
#include "iterator.h"

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
  SliceArrayBuilder(uint64_t n, int key_size, int index){
    this->a = new char[n * key_size];
    this->cur = 0;
    this->n = n;
    this->key_size = key_size;
    this->index_ = index;
  };
  void add(const Slice &t) override;
  Iterator<Slice> *finish() override;

private:
  char *a;
  int n;
  int cur;
  int key_size;
  int index_;
};

SliceArrayIterator::~SliceArrayIterator() { delete a_; }
bool SliceArrayIterator::valid() const { return cur_ < num_keys_; }
void SliceArrayIterator::next() {
  assert(valid());
  cur_++;
}
Slice SliceArrayIterator::peek(uint64_t pos) const {
  return Slice(a_ + pos * key_size_, key_size_);
}
void SliceArrayIterator::seek(Slice item) { abort(); }
void SliceArrayIterator::seekToFirst() { cur_ = 0; }
Slice SliceArrayIterator::key() {
  return Slice(a_ + cur_ * key_size_, key_size_);
}
uint64_t SliceArrayIterator::current_pos() const { return cur_; }

void SliceArrayBuilder::add(const Slice &t) {
  for (int i = 0; i < key_size; i++) {
    a[cur * key_size + i] = t.data_[i];
  }
  KEY_TYPE *val = (KEY_TYPE *)(a + cur * key_size);
  cur++;
}
Iterator<Slice> *SliceArrayBuilder::finish() {
  std::string iterator_id = "identifier_" + std::to_string(index_);
  return new SliceArrayIterator(a, n, key_size, iterator_id);
}

#endif
#include "slice_array_iterator.h"
#include "config.h"
#include "math.h"
#include "slice_comparator.h"
#include <iostream>

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

SliceArrayBuilder::SliceArrayBuilder(uint64_t n, int key_size, int index) {
  uint64_t bytes_to_allocate = n * key_size;
  this->a = new char[bytes_to_allocate];
  this->cur = 0;
  this->n = n;
  this->key_size = key_size;
  this->index_ = index;
}
void SliceArrayBuilder::add(const Slice &t) {
  for (int i = 0; i < key_size; i++) {
    uint64_t idx = cur * key_size + i;
    a[idx] = t.data_[i];
  }
  KEY_TYPE *val = (KEY_TYPE *)(a + cur * key_size);
  cur++;
}
Iterator<Slice> *SliceArrayBuilder::finish() {
  printf("Num Keys: %ld\n", n);
  std::string iterator_id = "identifier_" + std::to_string(index_);
  return new SliceArrayIterator(a, n, key_size, iterator_id);
}

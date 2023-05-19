#include "slice_array.h"
#include "slice_comparator.h"

SliceArrayIterator::SliceArrayIterator(char *a, int n, int key_size) {
  this->a = a;
  this->cur = 0;
  this->n = n;
  this->key_size = key_size;
}
SliceArrayIterator::~SliceArrayIterator() { delete a; }
bool SliceArrayIterator::valid() const { return cur < n; }
void SliceArrayIterator::next() {
  assert(valid());
  cur++;
}
Slice SliceArrayIterator::peek(uint64_t pos) const {
  return Slice(a + pos * key_size, key_size);
}
void SliceArrayIterator::seek(Slice item) {
  Comparator<Slice> *c = new SliceComparator();
  for (int i = 0; i < n; i++) {
    if (c->compare(Slice(a + i * key_size, key_size), item) > 0) {
      cur = i;
      return;
    }
  }
  cur = n;
}
void SliceArrayIterator::seekToFirst() { cur = 0; }
Slice SliceArrayIterator::key() const {
  return Slice(a + cur * key_size, key_size);
}
uint64_t SliceArrayIterator::current_pos() const { return cur; }

SliceArrayBuilder::SliceArrayBuilder(int n, int key_size) {
  this->a = new char[n * key_size];
  this->cur = 0;
  this->n = n;
  this->key_size = key_size;
}
void SliceArrayBuilder::add(const Slice& t) {
  for (int i = 0; i < key_size; i++) {
    a[cur * key_size + i] = t.data_[i];
  }
  cur++;
}
SliceArrayIterator *SliceArrayBuilder::finish() {
  return new SliceArrayIterator(a, n, key_size);
}

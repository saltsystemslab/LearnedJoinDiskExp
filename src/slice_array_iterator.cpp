#include "slice_array_iterator.h"
#include "config.h"
#include "math.h"
#include "slice_comparator.h"

SliceArrayIterator::~SliceArrayIterator() { delete a; }
bool SliceArrayIterator::valid() const { return cur < num_keys_; }
void SliceArrayIterator::next() {
  assert(valid());
  cur++;
}
Slice SliceArrayIterator::peek(uint64_t pos) const {
  return Slice(a + pos * key_size, key_size);
}
void SliceArrayIterator::seek(Slice item) { abort(); }
void SliceArrayIterator::seekToFirst() { cur = 0; }
Slice SliceArrayIterator::key() { return Slice(a + cur * key_size, key_size); }
uint64_t SliceArrayIterator::current_pos() const { return cur; }

SliceArrayBuilder::SliceArrayBuilder(int n, int key_size, int index) {
  this->a = new char[n * key_size];
  this->cur = 0;
  this->n = n;
  this->key_size = key_size;
  this->index_ = index;
#if LEARNED_MERGE
  plrBuilder = new PLRBuilder(PLR_ERROR_BOUND);
#endif
}
void SliceArrayBuilder::add(const Slice &t) {
  for (int i = 0; i < key_size; i++) {
    a[cur * key_size + i] = t.data_[i];
  }
  KEY_TYPE *val = (KEY_TYPE *)(a + cur * key_size);
  cur++;
#if LEARNED_MERGE
#if USE_STRING_KEYS
  plrBuilder->processKey(LdbKeyToInteger(t));
#else
  plrBuilder->processKey(*(KEY_TYPE *)(t.data_));
#endif
#endif
}
SliceArrayIterator *SliceArrayBuilder::finish() {
#if LEARNED_MERGE
  return new SliceArrayIterator(a, n, key_size, plrBuilder->finishTraining(),
                                index_);
#else
  return new SliceArrayIterator(a, n, key_size, nullptr, index_);
#endif
}

#include "comparator.h"
#include "iterator.h"
#include "merge.h"
#include "merge_result_builder.h"
#include "slice_comparator.h"
#include <cassert>
#include <iostream>

#define KEY_SIZE 19
using namespace std;

string generate_key(int key_value) {
  string key = to_string(key_value);
  string result = string(KEY_SIZE - key.length(), '0') + key;
  return std::move(result);
}

class ArraySliceIterator : public Iterator<Slice> {
public:
  ArraySliceIterator(char *a, int n, int key_size) {
    this->a = a;
    this->cur = 0;
    this->n = n;
    this->key_size = key_size;
  }
  ~ArraySliceIterator() { delete a; }
  bool valid() const override { return cur < n; }
  void next() override {
    assert(valid());
    cur++;
  }
  Slice peek(uint64_t pos) const override {
    return Slice(a + pos * key_size, key_size);
  }
  void seek(Slice item) override {
    Comparator<Slice> *c = new SliceComparator();
    for (int i = 0; i < n; i++) {
      if (c->compare(Slice(a + i * key_size, key_size), item) > 0) {
        cur = i;
        return;
      }
    }
    cur = n;
  }
  void seekToFirst() override { cur = 0; }
  Slice key() const override { return Slice(a + cur * key_size, key_size); }

private:
  char *a;
  int key_size;
  int cur;
  int n;
};

class SliceMergeResultBuilder : public MergeResultBuilder<Slice> {
public:
  SliceMergeResultBuilder(int n, int key_size) {
    this->a = new char[n * key_size];
    this->cur = 0;
    this->n = n;
    this->key_size = key_size;
  }
  void add(Slice t) override {
    for (int i = 0; i < key_size; i++) {
      a[cur * key_size + i] = t.data_[i];
    }
    cur++;
  }
  ArraySliceIterator *finish() {
    return new ArraySliceIterator(a, n, KEY_SIZE);
  }

private:
  char *a;
  int n;
  int cur;
  int key_size;
};

int main() {

  SliceMergeResultBuilder *builder1 = new SliceMergeResultBuilder(10, KEY_SIZE);
  for (int i = 0; i < 10; i++) {
    std::string key = generate_key(2 * i);
    builder1->add(Slice(key.c_str(), KEY_SIZE));
  }
  SliceMergeResultBuilder *builder2 = new SliceMergeResultBuilder(10, KEY_SIZE);
  for (int i = 0; i < 10; i++) {
    builder2->add(Slice(generate_key((2 * i + 1)).c_str(), KEY_SIZE));
  }

  Iterator<Slice> **iterators = new Iterator<Slice> *[2];
  iterators[0] = builder1->finish();
  iterators[1] = builder2->finish();

  while (iterators[0]->valid()) {
    iterators[0]->next();
  }

  while (iterators[1]->valid()) {
    iterators[1]->next();
  }
  Comparator<Slice> *c = new SliceComparator();

  SliceMergeResultBuilder *resultBuilder =
      new SliceMergeResultBuilder(20, KEY_SIZE);
  StandardMerger::merge(iterators, 2, c, resultBuilder);

  Iterator<Slice> *result = resultBuilder->finish();
  int i = 0;
  while (result->valid()) {
    assert(c->compare(result->key(),
                      Slice(generate_key((i)).c_str(), KEY_SIZE)) == 0);
    i++;
    result->next();
  }
  assert(i == 20);
  std::cout << "Ok!" << std::endl;
}

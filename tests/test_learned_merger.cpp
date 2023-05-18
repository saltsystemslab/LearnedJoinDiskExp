#include "comparator.h"
#include "iterator.h"
#include "learned_merge.h"
#include <cassert>
#include <iostream>

using namespace std;

class IntComparator : public Comparator<int> {
public:
  int compare(const int &a, const int &b) const override {
    if (a == b)
      return 0;
    if (a > b)
      return 1;
    return -1;
  }
};

class ArrayIntIterator : public Iterator<int> {
public:
  ArrayIntIterator(int *a, int n) {
    this->a = a;
    this->cur = 0;
    this->n = n;
  }
  ~ArrayIntIterator() { delete a; }
  bool valid() const override { return cur < n; }
  void next() override { cur++; }
  int peek(uint64_t pos) const override { return a[pos]; }
  void seek(int item) override {
    for (int i = 0; i < n; i++) {
      if (a[i] > item) {
        cur = i;
        return;
      }
    }
    cur = n;
  }
  void seekToFirst() override { cur = 0; }
  int key() const override { return a[cur]; }
  uint64_t current_pos() const override { return cur; }
  double guessPosition(int target_key) {
    for (int i = 0; i < n - 1; i++) {
      if (a[i] > target_key) {
        return cur;
      }
    }
    return cur;
  }

private:
  int *a;
  int cur;
  int n;
};

class IntArrayIteratorBuilder : public IteratorBuilder<int> {
public:
  IntArrayIteratorBuilder(int n) {
    this->a = new int[n];
    this->cur = 0;
    this->n = n;
  }
  void add(int t) override { a[cur++] = t; }
  ArrayIntIterator *finish() { return new ArrayIntIterator(a, n); }

private:
  int *a;
  int n;
  int cur;
};

int main() {
  IntArrayIteratorBuilder *builder1 = new IntArrayIteratorBuilder(10);
  for (int i = 0; i < 10; i++) {
    builder1->add(2 * i);
  }
  IntArrayIteratorBuilder *builder2 = new IntArrayIteratorBuilder(10);
  for (int i = 0; i < 10; i++) {
    builder2->add(2 * i + 1);
  }

  Iterator<int> **iterators = new Iterator<int> *[2];
  iterators[0] = builder1->finish();
  iterators[1] = builder2->finish();

  Comparator<int> *c = new IntComparator();

  IntArrayIteratorBuilder *resultBuilder = new IntArrayIteratorBuilder(20);
  LearnedMerger<int>::merge(iterators, 2, c, resultBuilder);

  Iterator<int> *result = resultBuilder->finish();
  int i = 0;
  while (result->valid()) {
    assert(result->key() == i++);
    result->next();
  }
  assert(i == 20);
  std::cout << "Ok!" << std::endl;
}

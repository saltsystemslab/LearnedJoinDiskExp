#ifndef INT_ITERATOR_H
#define INT_ITERATOR_H

#include "iterator.h"

class IntArrayIterator : public Iterator<int> {
public:
  IntArrayIterator(int *a, int n) {
    this->a = a;
    this->cur = 0;
    this->n = n;
  }
  ~IntArrayIterator() { delete a; }
  bool valid() const override { return cur < n; }
  void next() override {
    assert(valid());
    cur++;
  }
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
  int key() override { return a[cur]; }
  uint64_t current_pos() const override { return cur; }
  double guessPosition(int target_key) {
    for (int i = 0; i < n; i++) {
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
  void add(const int &t) override { a[cur++] = t; }
  IntArrayIterator *finish() override { return new IntArrayIterator(a, n); }

private:
  int *a;
  int n;
  int cur;
};

#endif

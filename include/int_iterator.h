#ifndef INT_ITERATOR_H
#define INT_ITERATOR_H

#include "config.h"
#include "iterator.h"

#include "pgm/pgm_index.hpp"
#include <chrono>
#include <vector>

static const int epsilon = PGM_ERROR_BOUND; // space-time trade-off parameter

template <class T> class IntArrayIterator : public Iterator<T> {
public:
  IntArrayIterator(std::vector<T> *a, int n) {
    this->a = a;
    this->cur = 0;
    this->n = n;
#if TRACK_PLR_TRAIN_TIME
    auto train_start = std::chrono::high_resolution_clock::now();
#endif
#if LEARNED_MERGE
    this->pgm_index = new pgm::PGMIndex<KEY_TYPE, epsilon>(*a);
#endif
#if TRACK_PLR_TRAIN_TIME
    auto train_end = std::chrono::high_resolution_clock::now();
    auto training_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                             train_end - train_start)
                             .count();
    std::cout << "Model Training time: " << training_time << std::endl;
#endif
  }
  ~IntArrayIterator() { delete a; }
  bool valid() const override { return cur < n; }
  void next() override {
    assert(valid());
    cur++;
  }
  T peek(uint64_t pos) const override { return a->at(pos); }
  void seek(T item) override {
    for (int i = 0; i < n; i++) {
      if (a->at(i) > item) {
        cur = i;
        return;
      }
    }
    cur = n;
  }
  void seekToFirst() override { cur = 0; }
  T key() override { return a->at(cur); }
  uint64_t current_pos() const override { return cur; }
  double guessPositionMonotone(T target_key) {
#if LEARNED_MERGE
    return pgm_index->search(target_key).lo;
#else
    return 1.0;
#endif
  }

private:
  std::vector<T> *a;
  int cur;
  int n;
#if LEARNED_MERGE
  pgm::PGMIndex<uint64_t, epsilon> *pgm_index;
#endif
};

template <class T> class IntArrayIteratorBuilder : public IteratorBuilder<T> {
public:
  IntArrayIteratorBuilder(int n) {
    this->a = new std::vector<T>(n);
    this->cur = 0;
    this->n = n;
  }
  void add(const T &t) override { (*a)[cur++] = t; }
  IntArrayIterator<T> *finish() override {
    return new IntArrayIterator<T>(a, n);
  }

private:
  std::vector<T> *a;
  int n;
  int cur;
};

template <class T> class IntComparator : public Comparator<T> {
public:
  int compare(const T &a, const T &b) override {
    if (a == b)
      return 0;
    if (a > b)
      return 1;
    return -1;
  }
};
#endif

#ifndef _MODEL_H
#define _MODEL_H

#include <stdint.h>

#include <cassert>
#include <string>

/**
Maybe this should actually be called is IteratorWithModel. But we need models
almost always, so model querying is exposed as an method.
*/
template <class T> class Model {
public:
  // Assumes that the queries for guessPosition are monotonically increasing.
  // The model lookup can be optimized to take advantage of that.
  virtual double guessPositionMonotone(T target_key) {
    printf("Uninmplemented guessPositionMonotone\n");
    abort();
  }
  virtual double guessPositionUsingBinarySearch(T target_key) { return -1; }
  virtual uint64_t getNumberOfSegments() {return 0;}
};

template <class T> class ModelBuilder {
public:
  virtual void add(const T &t) = 0;
  virtual Model<T> *finish() = 0;
};

#endif

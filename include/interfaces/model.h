#ifndef _MODEL_H
#define _MODEL_H

#include <stdint.h>

#include <cassert>
#include <string>

/* Represents a CDF. */
template <class T>
class Model {
 public:
  // Assumes that the queries for guessPosition are monotonically increasing.
  // The model lookup can be optimized to take advantage of that.
  virtual double guessPositionMonotone(T target_key) {
    printf("Uninmplemented guessPositionMonotone\n");
    abort();
  }
  virtual double guessPositionUsingBinarySearch(T target_key) { return -1; }
};

template <class T>
class ModelBuilder {
 public:
  virtual void add(const T &t) = 0;
  virtual Model<T> *finish() = 0;
};

template <class T>
class DummyModel : public Model<T> {
 public:
  DummyModel();
};

template <class T>
class DummyModelBuilder : public ModelBuilder<T> {
 public:
  void add(const T &t) override {}
  Model<T> *finish() override { return new DummyModel<T>(); }
};

#endif

#ifndef _MODEL_H
#define _MODEL_H

#include <stdint.h>

#include <cassert>
#include <string>

/* Represents a CDF. */
template <class T> class Model {
public:
  virtual uint64_t guessPosition(T target_key) = 0;
  // Assumes that the queries for guessPosition are monotonically increasing.
  // The model lookup can be optimized to take advantage of that.
  virtual uint64_t guessPositionMonotone(T target_key) {
    return guessPosition(target_key);
  }
  virtual Model<T> *get_model_for_subrange(uint64_t start, uint64_t end) = 0;
  // Returns the error window size 'e'. Items queried for in guessPosition
  // are expected (but not guaranteed) to be in [p-e, p+e]
  // We could consider guessPosition return a pair representing [p-e, p+e].
  // The problem is that not all modell guarantee items to be in the window
  // (PLR). So leave it to higher level interfaces to error correct.
  virtual double getMaxError() = 0;
  virtual uint64_t size_bytes() = 0;
};

template <class T> class ModelBuilder {
public:
  virtual void add(const T &t) = 0;
  virtual Model<T> *finish() = 0;
};

template <class T> class DummyModel : public Model<T> {
public:
  DummyModel() {}
  uint64_t guessPosition(T target_key) override { abort(); };
  uint64_t guessPositionMonotone(T target_key) override { abort(); }
  Model<T> *get_model_for_subrange(uint64_t start, uint64_t end) override {
    abort();
  };
  double getMaxError() override { abort(); };
  uint64_t size_bytes() override { return 0; };
};

template <class T> class DummyModelBuilder : public ModelBuilder<T> {
public:
  void add(const T &t) override {}
  Model<T> *finish() override { return new DummyModel<T>(); }
};

#endif

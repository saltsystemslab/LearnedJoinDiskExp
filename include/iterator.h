#ifndef ITERATOR_H
#define ITERATOR_H

#include <stdint.h>

#include <cassert>
#include <string>

/**
Maybe this should actually be called is IteratorWithModel. But we need models
almost always, so model querying is exposed as an method.
*/
template <class T> class Iterator {
public:
  virtual bool valid() const = 0;
  virtual void next() = 0;
  virtual T peek(uint64_t pos) const = 0;
  virtual void seek(T item) = 0;
  virtual void seekToFirst() = 0;
  virtual T key() = 0;
  virtual uint64_t current_pos() const = 0;
  // Assumes that the queries for guessPosition are monotonically increasing.
  // The model lookup can be optimized to take advantage of that.
  virtual double guessPositionMonotone(T target_key) {
    printf("Uninmplemented guessPositionMonotone\n");
    abort();
  }
  virtual std::string identifier() { return "unnamed_iterator"; }
  /*
   * Returns number of keys copied. Data must be valid until next bulkRead call.
   */
  virtual uint64_t bulkReadAndForward(uint64_t num_keys, char **data,
                                      uint64_t *len) {
    printf("Unimplemented bulkReadAndForward!\n");
    abort();
  };
};

template <class T> class IteratorBuilder {
public:
  virtual void add(const T &t) = 0;
  /* Also advances the iterator. Assumes iterator will be valid for so many
   * keys*/
  virtual void bulkAdd(Iterator<T> *iter, uint64_t num_keys) {
    for (int i = 0; i < num_keys; i++) {
      add(iter->key());
      iter->next();
    }
  }
  virtual Iterator<T> *finish() = 0;
};

#endif

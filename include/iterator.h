#ifndef ITERATOR_H
#define ITERATOR_H

#include <stdint.h>

#include <cassert>
#include <string>

template <class T> class Iterator {
public:
  virtual bool valid() const = 0;
  virtual void next() = 0;
  virtual T peek(uint64_t pos) const = 0;
  virtual void seek(T item) = 0;
  virtual void seekToFirst() = 0;
  virtual T key() = 0;
  virtual uint64_t current_pos() const = 0;
  virtual double guessPosition(T target_key) { return -1; }
  virtual double guessPositionUsingBinarySearch(T target_key) { return -1; }
  virtual int index() { return -1; }
  /*
   * Returns number of keys copied. Data must be valid until next bulkRead call.
   */
  virtual uint64_t bulkReadAndForward(uint64_t num_keys, char **data,
                                      uint64_t *len) {
    printf("Not implemented!\n");
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

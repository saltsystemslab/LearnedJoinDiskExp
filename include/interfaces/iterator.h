#ifndef _ITERATOR_H
#define _ITERATOR_H

#include <stdint.h>

#include <cassert>
#include <string>

template <class T> class Iterator {
public:
  virtual bool valid() const = 0;
  virtual void next() = 0;
  virtual T peek(uint64_t pos) const = 0;
  virtual void seekToFirst() = 0;
  virtual void seekToPos(uint64_t pos) = 0;
  virtual T key() = 0;
  virtual uint64_t current_pos() const = 0;
  virtual uint64_t num_keys() const = 0;
  virtual std::string id() { return "unnamed_iterator"; }
  // Attempts to load upto num_keys into a buffer.
  // data is set to point to this buffer, and len is actual number of bytes
  // read. data is valid until next call to bulkReadAndForward. Returns the
  // number of keys actually read. The iterator is moved ahead by this much.
  virtual uint64_t bulkReadAndForward(uint64_t num_keys, char **data,
                                      uint64_t *len) = 0;
  // Return an iterator that serves items from [start, end).
  virtual Iterator<T> *subRange(uint64_t start, uint64_t end) = 0;
};

template <class T> class IteratorBuilder {
public:
  virtual void add(const T &t) = 0;
  virtual Iterator<T> *build() = 0;
  virtual void bulkAdd(Iterator<T> *iter, uint64_t num_keys) = 0;
  // Return an IteratorBuilder that will write items to subrange [start, end).
  // When using subRange, add and bulkAdd should NOT be used.
  // The returned IteratorBuilder should be used as a normal IteratorBuilder
  // (use add or bulkAdd). Calling build() after all subRange have been built
  // should return an iterator which includes all iterators built by the
  // subRange builders.
  virtual IteratorBuilder<T> *subRange(uint64_t start, uint64_t end) = 0;
};

#endif

#ifndef ITERATOR_H
#define ITERATOR_H

#include <cassert>
#include <stdint.h>

template <class T> class Iterator {
public:
  virtual bool valid() const = 0;
  virtual void next() = 0;
  virtual T peek(uint64_t pos) const = 0;
  virtual void seek(T item) = 0;
  virtual void seekToFirst() = 0;
  virtual T key() const = 0;
  virtual uint64_t current_pos() const = 0;
  virtual double guessPosition(T target_key) { return -1; }
};

template <class T> class IteratorBuilder {
public:
  virtual void add(const T &t) = 0;
  virtual Iterator<T> *finish() = 0;
};

#endif

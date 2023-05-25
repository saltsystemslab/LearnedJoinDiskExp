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
  virtual int index() { return -1; }
};

template <class T> class IteratorBuilder {
public:
  virtual void add(const T &t) = 0;
  virtual Iterator<T> *finish() = 0;
};

#endif

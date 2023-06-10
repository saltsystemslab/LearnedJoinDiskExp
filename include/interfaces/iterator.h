#ifndef _ITERATOR_H
#define _ITERATOR_H

#include <stdint.h>

#include <cassert>
#include <string>

template <class T>
class Iterator {
 public:
  virtual bool valid() const = 0;
  virtual void next() = 0;
  virtual T peek(uint64_t pos) const = 0;
  virtual void seekToFirst() = 0;
  virtual T key() = 0;
  virtual uint64_t current_pos() const = 0;
  virtual uint64_t num_keys() const = 0;
  virtual std::string id() { return "unnamed_iterator"; }
  virtual uint64_t bulkReadAndForward(uint64_t num_keys, char **data,
                                      uint64_t *len) {
    printf("Unimplemented bulkReadAndForward!\n");
    abort();
  };
};

template <class T>
class BulkReadIterator {
 public:
  virtual bool valid() const = 0;
  virtual uint64_t current_pos() const = 0;
  virtual uint64_t num_keys() const = 0;
  virtual T key() = 0;
  virtual std::string id() { return "unnamed_iterator"; }
  // Attempts to load upto num_keys into a buffer.
  // data is set to point to this buffer, and len is actual number of bytes
  // read. data is valid until next call to bulkReadAndForward. Returns the
  // number of keys actually read. The iterator is moved ahead by this much.
  virtual uint64_t bulkReadAndForward(uint64_t num_keys, char **data,
                                      uint64_t *len) = 0;
};

template <class T>
class IteratorBuilder {
 public:
  virtual void add(const T &t) = 0;
  virtual void bulkAdd(Iterator<T> *iter, int num_keys) {
    for (int i = 0; i < num_keys; i++) {
      add(iter->key());
      iter->next();
    }
  }
  virtual Iterator<T> *build() = 0;
  virtual BulkReadIterator<T> *buildBulkIterator() = 0;
};

template <class T>
class BulkAddIteratorBuilder {
  // Add num_keys from iter. Advnaces the iter by num_keys.
  // Assumes iter is valid for num_keys.
  virtual void bulkAdd(BulkReadIterator<T> *iter, uint64_t num_keys) override;
  virtual Iterator<T> *buildIterator() = 0;
};

#endif

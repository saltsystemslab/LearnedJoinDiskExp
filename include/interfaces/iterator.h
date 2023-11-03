#ifndef LEARNEDINDEXMERGE_ITERATOR_H
#define LEARNEDINDEXMERGE_ITERATOR_H

#include <stdint.h>

namespace li_merge {
template <class T> class Iterator {
public:
  virtual bool valid() = 0;
  virtual void next() = 0;
  virtual T peek(uint64_t pos) = 0;
  virtual void seekToFirst() = 0;
  virtual void seekTo(uint64_t pos) = 0;
  virtual T key() = 0;
  virtual uint64_t currentPos() = 0;
  virtual uint64_t numElts() = 0;
  // STATS.
  virtual uint64_t getDiskFetches() { return 0; };
};

template <class T>
struct Window {
  uint64_t lo_idx;
  uint64_t hi_idx;
  char *buf;
  uint64_t buf_len;
  Iterator<T> *iter;
};

template <class T> class WindowIterator {
  public:
    // Hint as to how big the size of the inner buffer should be.
    virtual void setWindowSize(uint64_t num_keys) = 0;
    // Guaranteed to return lo_idx. Max keys returned will contain hi_idx (could be lesser).
    virtual Window<T> getWindow(uint64_t lo_idx, uint64_t hi_idx) = 0;
    virtual uint64_t getDiskFetches() = 0;
};
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_ITERATOR_H

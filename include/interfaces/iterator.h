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
  virtual T key() = 0;
  // Attempt to load (current_pos, to pos). Returns a pointer and number of elements.
  virtual void peekAhead(uint64_t pos, char **buf, int *len) {};
  // Load (lower, upper) into buffer provided by user
  // if lower < 0, it will be filled with 0. if upper > lim, it will be filled with 1s.
  virtual void loadWindow(char *buf, uint64_t lower, uint64_t upper) {};
  virtual uint64_t currentPos() = 0;
  virtual uint64_t numElts() = 0;
  // STATS.
  virtual uint64_t getDiskFetches() { return 0; };
  virtual bool checkCache(T key) { return false; };
};
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_ITERATOR_H

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
  virtual uint64_t current_pos() = 0;
  virtual uint64_t num_elts() = 0;
};
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_ITERATOR_H

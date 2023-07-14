#ifndef LEARNEDINDEXMERGE_INDEX_H
#define LEARNEDINDEXMERGE_INDEX_H

#include <unistd.h>

namespace li_merge {
struct Bounds {
  uint64_t lower;
  uint64_t upper;
  uint64_t approx_pos;
};

template <class T> class Index {
  virtual uint64_t getExactPosition(const T &t) = 0;
  virtual Bounds getPositionBounds(const T &t) = 0;
};
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_INDEX_H

#ifndef SLICE_COMPARATOR_H
#define SLICE_COMPARATOR_H

#include "comparator.h"
#include "config.h"
#include "slice.h"
#include <cstring>

class SliceComparator : public Comparator<Slice> {
public:
  int compare(const Slice &a, const Slice &b) override {
    const size_t min_len = (a.size_ < b.size_) ? a.size_ : b.size_;
    int r = memcmp(a.data_, b.data_, min_len);
    if (r == 0) {
      if (a.size_ < b.size_)
        r = -1;
      else if (a.size_ > b.size_)
        r = +1;
    }
    return r;
  }
};

#endif

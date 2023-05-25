#ifndef SLICE_COMPARATOR_H
#define SLICE_COMPARATOR_H

#include "comparator.h"
#include "config.h"
#include "slice.h"
#include <cstring>

class SliceComparator : public Comparator<Slice> {
public:
  int compare(const Slice &a, const Slice &b) override {
#if USE_STRING_KEYS
    const size_t min_len = (a.size_ < b.size_) ? a.size_ : b.size_;
    int r = memcmp(a.data_, b.data_, min_len);
    if (r == 0) {
      if (a.size_ < b.size_)
        r = -1;
      else if (a.size_ > b.size_)
        r = +1;
    }
    return r;
#else
    KEY_TYPE *k1 = (KEY_TYPE *)a.data_;
    KEY_TYPE *k2 = (KEY_TYPE *)b.data_;
    if (*k1 == *k2) return 0;
    if (*k1 < *k2) return -1;
    return 1;
#endif

  }
};

#endif

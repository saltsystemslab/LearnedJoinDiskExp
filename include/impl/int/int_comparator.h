#ifndef INT_COMPARATOR_H
#define INT_COMPARATOR_H

#include "comparator.h"
#include "config.h"
#include "slice.h"
#include <cstring>

template <class T> class IntComparator : public Comparator<T> {
public:
  int compare(const T &a, const T &b) override {
    if (a == b)
      return 0;
    if (a < b)
      return -1;
    return 1;
  }
};

#endif

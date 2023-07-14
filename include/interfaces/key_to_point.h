#ifndef LEARNEDINDEXMERGE_KEY_TO_POINT_H
#define LEARNEDINDEXMERGE_KEY_TO_POINT_H

#include "config.h"

namespace li_merge {
template <class T> class KeyToPointConverter {
public:
  virtual POINT_FLOAT_TYPE toPoint(const T &t) = 0;
};
}; // namespace li_merge

#endif
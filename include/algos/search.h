#ifndef SEARCH_H
#define SEARCH_H

#include "iterator.h"
#include "index.h"

namespace li_merge {

struct SearchResult {
  uint64_t lower_bound;
  bool found;
  bool shouldContinue;
};

template <class T>
class SearchStrategy {
  public:
    virtual SearchResult search(Window<T> window, T Key, Bounds bound) = 0;
};

class LinearSearch: public SearchStrategy<KVSlice> {
public:
  SearchResult search(Window<KVSlice> window, KVSlice kv, Bounds bound) override {
    uint64_t i;
    bool found = false;
    for (i=0; i < window.buf_len; i+=16) {
      if(memcmp(window.buf + i, kv.data(), 8) == 0) {
        found = true;
        break;
      }
    }
    if (found) {
      return {
        window.lo_idx + i,
        true,
        false
      };
    }
    // The last key checked is greater than the bounds, exit.
    if(window.hi_idx  >= bound.upper) {
      return {
        window.lo_idx + i,
        false,
        false
      };
    } else {
      return {
        window.lo_idx + i,
        true,
        true
      };
    }
  }
};

}
#endif

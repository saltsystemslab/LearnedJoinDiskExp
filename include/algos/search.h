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
    if (bound.lower >= bound.upper) {
      return {
        bound.lower,
        false,
        false
      };
    }
    uint64_t i;
    bool found = false;
    uint64_t key = *(uint64_t *)kv.data();
    for (i=0; i < window.buf_len; i+=16) {
      uint64_t candidate = *(uint64_t *)(window.buf + i);
      if (candidate < key) {
        continue;
      }
      if (candidate == key) {
        return {
          window.lo_idx + i/16,
          true,
          false
        };
      }
      if (candidate > key) {
        return {
          window.lo_idx + i/16,
          false,
          false
        };
      }
    }
    return {
      window.lo_idx + i/16,
      false,
      true
    };
  }
};

}
#endif

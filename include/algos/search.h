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

// TODO(chesetti): Templatize. Right now hardcoded to 64 byte integer keys.
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

// TODO(chesetti): Templatize, right now hardcoded for 8 + 8 64 byte keys.
class BinarySearch : public SearchStrategy<KVSlice> {
private:
  inline size_t bit_floor(size_t i) {
    constexpr int num_bits = sizeof(i) * 8;
    return size_t(1) << (num_bits - std::countl_zero(i) - 1);
  }

  inline size_t bit_ceil(size_t i) {
    constexpr int num_bits = sizeof(i) * 8;
    return size_t(1) << (num_bits - std::countl_zero(i - 1));
  }

  uint64_t find_lower_bound(char *buf, uint64_t len_in_bytes, uint64_t key_value) {
    uint64_t start = 0;
    int kvSize = 16;
    uint64_t end = (len_in_bytes) / (kvSize);
    uint64_t len = end - start;
    uint64_t step = bit_floor(len);
    uint64_t cur = *(uint64_t *)(buf + (kvSize) * (step + start));
    if (step != len && (cur < key_value)) {
      len -= step + 1;
      if (len == 0) {
        return 0;
      }
      step = bit_ceil(len);
      start = end - step;
    }
    for (step /= 2; step != 0; step /= 2) {
      cur = *(uint64_t *)(buf + (kvSize) * (step + start));
      if (cur < key_value)
        start += step;
    }
    cur = *(uint64_t *)(buf + (kvSize) * (step + start));
    start = start + (cur < key_value);
    return start;
  }



public:
  SearchResult search(Window<KVSlice> window, KVSlice kv, Bounds bound) override {
    if (bound.lower >= bound.upper) {
      return {
        bound.lower,
        false,
        false
      };
    }
    int kvSize = 16;
    uint64_t key_value = *(uint64_t *)(kv.data());
    uint64_t last_key = *(uint64_t *)(window.buf + window.buf_len - kvSize);
    if (last_key < key_value) {
      return {
        window.hi_idx,
        false, // found
        true  // continue
      };
    }
    uint64_t lower_bound = find_lower_bound(window.buf, window.buf_len, key_value);
    uint64_t lower_bound_key = *(uint64_t *)(window.buf + lower_bound * kvSize);
    if (lower_bound_key == key_value) {
      return {
        window.lo_idx + lower_bound,
        true, // found
        false // continue
      };
    } else {
      return {
        window.lo_idx + lower_bound,
        false, // found
        false // continue
      };
    }
  }
};



}
#endif

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
	int countLeadingZeros(uint64_t number) {
    if (number == 0) return sizeof(number)*8;
    return __builtin_clzll(number);
	}

  inline size_t bit_floor(size_t i) {
    constexpr int num_bits = sizeof(i) * 8;
    return size_t(1) << (num_bits - countLeadingZeros(i) - 1);
  }

  inline size_t bit_ceil(size_t i) {
    constexpr int num_bits = sizeof(i) * 8;
    return size_t(1) << (num_bits - countLeadingZeros(i - 1));
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

class ExponentialSearch : public SearchStrategy<KVSlice> {
private:
  void getBoundForward(char *buf, uint64_t sz, uint64_t key, uint64_t *bound_lo, uint64_t *bound_hi) {
    // Last value that is lesser than or equal to lo
    if (sz == 0) {
      abort();
    }
    uint64_t bound = 1;
    while (bound < sz) {
      uint64_t cur = *(uint64_t *)(buf + 16 * (bound));
      if (cur < key) {
        bound = bound * 2;
      } else break;
    }
    *bound_lo = (bound/2);
    *bound_hi = std::min(bound + 1, sz);
  }

  // hi is inclusive, it is the index of the last key.
  void getBoundBackward(char *buf, uint64_t hi, uint64_t key, uint64_t *bound_lo, uint64_t *bound_hi) {
    // Last value that is lesser than or equal to lo
    if (hi == -1) {
      abort();
    }
    uint64_t bound = 1;
    while (bound < hi) {
      uint64_t cur = *(uint64_t *)(buf + 16 * (hi - bound));
      if (cur >= key) {
        bound = bound * 2;
      } else break;
    }
    *bound_hi = (hi - bound/2) + 1;
    if (bound > hi) {
      *bound_lo = 0;
    } else {
      *bound_lo = (hi - (bound + 1)) + 1;
    }
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
    uint64_t first_key = *(uint64_t *)(window.buf);
    uint64_t last_key = *(uint64_t *)(window.buf + window.buf_len - kvSize);
    // First Key is greater, no point searching.
    if (first_key > key_value) {
      return {
        window.lo_idx,
        false, // found
        false  // continue
      };

    }
    // Last key is less, continue search ahead.
    if (last_key < key_value) {
      return {
        window.hi_idx,
        false, // found
        true  // continue
      };
    }

    // We know the key is in this window. Let's start from Position.
    // First clamp it in [lo, hi_idx-1]
    uint64_t pos = std::clamp(bound.approx_pos, window.lo_idx, window.hi_idx-1) - window.lo_idx;
    uint64_t pos_key = *(uint64_t *)(window.buf + pos * kvSize);

    uint64_t lo;
    uint64_t hi;
    Window<KVSlice> bs_window;
    if (pos_key < key_value) {
      getBoundForward(window.buf + (pos * 16), (window.buf_len - pos*16)/16, key_value, &lo, &hi);
      bs_window.buf = window.buf + (lo+pos) * kvSize;
      bs_window.buf_len = (hi - lo) * (kvSize);
      bs_window.lo_idx = window.lo_idx + lo + pos;
      bs_window.hi_idx = window.lo_idx + hi + pos;
    } else {
      getBoundBackward(window.buf, pos, key_value, &lo, &hi);
      bs_window.buf = window.buf + (lo * kvSize);
      bs_window.buf_len = (hi - lo) * (kvSize);
      bs_window.lo_idx = window.lo_idx + lo;
      bs_window.hi_idx = window.lo_idx + hi;
    }
    return bs.search(bs_window, kv, bound);
  }
private:
  BinarySearch bs;

};



}
#endif

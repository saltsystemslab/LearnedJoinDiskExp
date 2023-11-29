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
    virtual SearchResult search(Window<T> window, T Key, Bounds bound, Comparator<T> *c) = 0;
};

// TODO(chesetti): Templatize. Right now hardcoded to 64 byte integer keys.
class LinearSearch: public SearchStrategy<KVSlice> {
public:
  SearchResult search(Window<KVSlice> window, KVSlice kv, Bounds bound, Comparator<KVSlice> *c) override {
    if (bound.lower >= bound.upper) {
      return {
        bound.lower,
        false,
        false
      };
    }
    uint64_t i;
    bool found = false;
    for (i=0; i < window.buf_len; i+=kv.total_size_bytes()) {
      KVSlice candidate(window.buf + i, kv.key_size_bytes(), kv.value_size_bytes());
      int compResult = c->compare(candidate, kv);
      if (compResult < 0) {
        continue;
      }
      if (compResult == 0) {
        return {
          window.lo_idx + i/kv.total_size_bytes(),
          true,
          false
        };
      }
      if (compResult > 0) {
        return {
          window.lo_idx + i/kv.total_size_bytes(),
          false,
          false
        };
      }
    }
    return {
      window.lo_idx + i/kv.total_size_bytes(),
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

  uint64_t find_lower_bound(char *buf, uint64_t len_in_bytes, KVSlice kv, Comparator<KVSlice> *c) {
    uint64_t start = 0;
    int kvSize = kv.total_size_bytes();
    uint64_t end = (len_in_bytes) / (kvSize);
    uint64_t len = end - start;
    uint64_t step = bit_floor(len);
    KVSlice cur1(buf + (kvSize) * (step + start), kv.key_size_bytes(), kv.value_size_bytes());
    if (step != len && (c->compare(cur1, kv) < 0)) {
      len -= step + 1;
      if (len == 0) {
        return 0;
      }
      step = bit_ceil(len);
      start = end - step;
    }
    for (step /= 2; step != 0; step /= 2) {
      KVSlice cur(buf + (kvSize) * (step + start), kv.key_size_bytes(), kv.value_size_bytes());
      if (c->compare(cur, kv) < 0)
        start += step;
    }
    KVSlice cur(buf + (kvSize) * (step + start), kv.key_size_bytes(), kv.value_size_bytes());
    start = start + (c->compare(cur, kv) < 0 ? 1: 0);
    return start;
  }

public:
  SearchResult search(Window<KVSlice> window, KVSlice kv, Bounds bound, Comparator<KVSlice> *c) override {
    if (bound.lower >= bound.upper) {
      return {
        bound.lower,
        false,
        false
      };
    }
    int kvSize = kv.total_size_bytes();
    KVSlice last_key(window.buf + window.buf_len - kvSize, kv.key_size_bytes(), kv.value_size_bytes());
    if (c->compare(last_key, kv) < 0) {
      return {
        window.hi_idx,
        false, // found
        true  // continue
      };
    }
    uint64_t lower_bound = find_lower_bound(window.buf, window.buf_len, kv, c);
    KVSlice lowerBoundKey(window.buf + lower_bound * kvSize, kv.key_size_bytes(), kv.value_size_bytes());
    if (c->compare(lowerBoundKey, kv) == 0) {
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
  SearchResult search(Window<KVSlice> window, KVSlice kv, Bounds bound, Comparator<KVSlice> *c) override {
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
    return bs.search(bs_window, kv, bound, c);
  }
private:
  BinarySearch bs;

};



}
#endif

#ifndef LEARNEDINDEXMERGE_KEY_VALUE_SLICE_H
#define LEARNEDINDEXMERGE_KEY_VALUE_SLICE_H

#include <cstring>

#include "comparator.h"

namespace li_merge {
class KVSlice {
 public:
  KVSlice() : data_(nullptr), key_size_bytes_(0), value_size_bytes_(0) {}
  KVSlice(const char *data, int key_size_bytes, int value_size_bytes)
      : data_(data),
        key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes) {}
  int key_size_bytes() const { return key_size_bytes_; }
  int value_size_bytes() const { return value_size_bytes_; }
  int total_size_bytes() const { return key_size_bytes_ + value_size_bytes_; }
  const char *data() const { return data_; }

  static KVSlice copy_kvslice(char *copy_buf, const KVSlice &kv) {
    memcpy(copy_buf, kv.data(), kv.total_size_bytes());
    return KVSlice(copy_buf, kv.key_size_bytes(), kv.value_size_bytes());
  }

 private:
  int key_size_bytes_;
  int value_size_bytes_;
  const char *data_;
};

class KVSliceMemcmp : public Comparator<KVSlice> {
 public:
  int compare(const KVSlice &a, const KVSlice &b) override {
    const size_t min_len = (a.key_size_bytes() < b.key_size_bytes())
                               ? a.key_size_bytes()
                               : b.key_size_bytes();
    int r = memcmp(a.data(), b.data(), min_len);
    if (r == 0) {
      if (a.key_size_bytes() < b.key_size_bytes())
        r = -1;
      else if (a.key_size_bytes() > b.key_size_bytes())
        r = +1;
    }
    return r;
  }
};

class KVUint64Cmp : public Comparator<KVSlice> {
 public:
  int compare(const KVSlice &a, const KVSlice &b) override {
    assert(a.key_size_bytes() == sizeof(uint64_t));
    assert(b.key_size_bytes() == sizeof(uint64_t));
    uint64_t *ak = (uint64_t *)(a.data());
    uint64_t *bk = (uint64_t *)(b.data());
    if (*ak < *bk) {
      return -1;
    }
    if (*ak > *bk) {
      return 1;
    }
    return 0;
  }
};

}  // namespace li_merge

#endif  // LEARNEDINDEXMERGE_KEY_VALUE_SLICE_H

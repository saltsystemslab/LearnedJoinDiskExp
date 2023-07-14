#ifndef LEARNEDINDEXMERGE_IN_MEM_SSTABLE_H
#define LEARNEDINDEXMERGE_IN_MEM_SSTABLE_H

#include "comparator.h"
#include "key_value_slice.h"
#include "sstable.h"
#include <cassert>
#include <vector>

namespace li_merge {

class FixedSizeKVInMemSSTable : public SSTable<KVSlice> {
public:
  FixedSizeKVInMemSSTable(char *data, int key_size_bytes, int value_size_bytes,
                          uint64_t num_kv)
      : data_(data), key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes), num_kv_(num_kv) {}
  Iterator<KVSlice> *iterator() { return nullptr; };

private:
  char *data_;
  uint64_t num_kv_;
  int key_size_bytes_;
  int value_size_bytes_;
};

class FixedSizeKVInMemSSTableBuilder : public SSTableBuilder<KVSlice> {
public:
  FixedSizeKVInMemSSTableBuilder(uint64_t approx_num_keys, int key_size_bytes,
                                 int value_size_bytes,
                                 Comparator<KVSlice> *comparator)
      : key_size_bytes_(key_size_bytes), value_size_bytes_(value_size_bytes),
        comparator_(comparator), num_kv_(0),
        last_key_buf_(new char[key_size_bytes + value_size_bytes]) {
    data_.reserve((approx_num_keys * (key_size_bytes + value_size_bytes)));
  }

  ~FixedSizeKVInMemSSTableBuilder() { delete[] last_key_buf_; }

  void add(const KVSlice &kv) override {
    assert(kv.key_size_bytes() == key_size_bytes_);
    assert(kv.value_size_bytes() == value_size_bytes_);
    char *kv_data = kv.data();
    for (int k_offset = 0; k_offset < key_size_bytes_; k_offset++) {
      data_.push_back(*(kv_data + k_offset));
    }
    for (int v_offset = key_size_bytes_; v_offset < kv.total_size_bytes();
         v_offset++) {
      data_.push_back(*(kv_data + v_offset));
    }
    assert(num_kv_ == 0 || comparator_->compare(kv, last_kv_) >= 0);
    last_kv_ = KVSlice::copy_kvslice(last_key_buf_, kv);
    num_kv_++;
  }

  SSTable<KVSlice> *build() override {
    return new FixedSizeKVInMemSSTable(data_.data(), key_size_bytes_,
                                       value_size_bytes_, num_kv_);
  }

private:
  std::vector<char> data_;
  int key_size_bytes_;
  int value_size_bytes_;
  uint64_t num_kv_;
  char *last_key_buf_;
  KVSlice last_kv_;
  Comparator<KVSlice> *comparator_;
};

/*
template <class T>
class FixedSizeKVInMemSSTableIterator : public Iterator<KVSlice> {
  FixedSizeKVInMemSSTableIterator(char *data, int key_size_bytes,
                                  int value_size_bytes)
      : data_(data), key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes);

private:
  char *data_;
  int key_size_bytes_;
  int value_size_bytes_;
};
*/
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_IN_MEM_SSTABLE_H

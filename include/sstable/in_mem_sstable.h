#ifndef LEARNEDINDEXMERGE_IN_MEM_SSTABLE_H
#define LEARNEDINDEXMERGE_IN_MEM_SSTABLE_H

#include <cassert>
#include <vector>

#include "comparator.h"
#include "key_value_slice.h"
#include "sstable.h"

namespace li_merge {

class FixedSizeKVInMemSSTable : public SSTable<KVSlice> {
public:
  FixedSizeKVInMemSSTable(char *data, int key_size_bytes, int value_size_bytes,
                          uint64_t num_kv)
      : data_(data), key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes), num_kv_(num_kv) {}
  Iterator<KVSlice> *iterator() override;

private:
  char *data_;
  uint64_t num_kv_;
  int key_size_bytes_;
  int value_size_bytes_;
};

class FixedSizeKVInMemSSTableBuilder : public SSTableBuilder<KVSlice> {
public:
  FixedSizeKVInMemSSTableBuilder(char *data, int key_size_bytes,
                                 int value_size_bytes,
                                 Comparator<KVSlice> *comparator)
      : data_(data), key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes), comparator_(comparator),
        num_kv_(0), last_key_buf_(new char[key_size_bytes + value_size_bytes]) {
  }

  static FixedSizeKVInMemSSTableBuilder *
  InMemMalloc(uint64_t num_keys, int key_size_bytes, int value_size_bytes,
              Comparator<KVSlice> *comparator) {
    char *data = new char[num_keys * (key_size_bytes + value_size_bytes)];
    return new FixedSizeKVInMemSSTableBuilder(data, key_size_bytes,
                                              value_size_bytes, comparator);
  }
  ~FixedSizeKVInMemSSTableBuilder() { delete[] last_key_buf_; }
  void add(const KVSlice &kv) override;
  void addWindow(const Window<KVSlice> &w) override { abort(); };
  SSTable<KVSlice> *build() override;

private:
  char *data_;
  int key_size_bytes_;
  int value_size_bytes_;
  uint64_t num_kv_;
  char *last_key_buf_;
  KVSlice last_kv_;
  Comparator<KVSlice> *comparator_;
};

class FixedSizeKVInMemSSTableIterator : public Iterator<KVSlice> {
public:
  FixedSizeKVInMemSSTableIterator(char *data, int key_size_bytes,
                                  int value_size_bytes, uint64_t num_keys)
      : data_(data), key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes), num_keys_(num_keys), cur_idx_(0) {}
  bool valid() override { return cur_idx_ < num_keys_; }
  void next() override { cur_idx_++; }
  KVSlice peek(uint64_t pos) override {
    return KVSlice(get_kv_offset(pos), key_size_bytes_, value_size_bytes_);
  }
  void seekToFirst() override { cur_idx_ = 0; }
  void seekTo(uint64_t pos) override { cur_idx_ = pos; }
  KVSlice key() override {
    return KVSlice(get_kv_offset(cur_idx_), key_size_bytes_, value_size_bytes_);
  }
  uint64_t currentPos() override { return cur_idx_; }
  uint64_t numElts() override { return num_keys_; }

private:
  char *data_;
  uint64_t num_keys_;
  int key_size_bytes_;
  int value_size_bytes_;
  uint64_t cur_idx_;
  inline char *get_kv_offset(uint64_t pos) {
    return data_ + (pos * (key_size_bytes_ + value_size_bytes_));
  }
};

class PFixedSizeKVInMemSSTableBuilder : public PSSTableBuilder<KVSlice> {
public:
  PFixedSizeKVInMemSSTableBuilder(uint64_t total_keys, int key_size_bytes,
                                  int value_size_bytes,
                                  Comparator<KVSlice> *comparator)
      : key_size_bytes_(key_size_bytes), value_size_bytes_(value_size_bytes),
        comparator_(comparator), total_kv_(total_keys) {
    data_ = new char[total_keys * (key_size_bytes + value_size_bytes)];
  }
  SSTableBuilder<KVSlice> *getBuilderForRange(uint64_t start_index,
                                              uint64_t end_index) override {
    return new FixedSizeKVInMemSSTableBuilder(
        data_ + (start_index * (key_size_bytes_ + value_size_bytes_)),
        key_size_bytes_, value_size_bytes_, comparator_);
  };
  SSTable<KVSlice> *build() override {
    return new FixedSizeKVInMemSSTable(data_, key_size_bytes_,
                                       value_size_bytes_, total_kv_);
  };

private:
  char *data_;
  int key_size_bytes_;
  int value_size_bytes_;
  uint64_t total_kv_;
  Comparator<KVSlice> *comparator_;
};

void FixedSizeKVInMemSSTableBuilder::add(const KVSlice &kv) {
  assert(kv.key_size_bytes() == key_size_bytes_);
  assert(kv.value_size_bytes() == value_size_bytes_);
  const char *kv_data = kv.data();
  memcpy(data_ + num_kv_ * kv.total_size_bytes(), kv_data,
         kv.total_size_bytes());
  assert(num_kv_ == 0 || comparator_ == nullptr ||
         comparator_->compare(kv, last_kv_) >= 0);
  last_kv_ = KVSlice::copy_kvslice(last_key_buf_, kv);
  num_kv_++;
}
SSTable<KVSlice> *FixedSizeKVInMemSSTableBuilder::build() {
  return new FixedSizeKVInMemSSTable(data_, key_size_bytes_, value_size_bytes_,
                                     num_kv_);
}

Iterator<KVSlice> *FixedSizeKVInMemSSTable::iterator() {
  return new FixedSizeKVInMemSSTableIterator(data_, key_size_bytes_,
                                             value_size_bytes_, num_kv_);
};

} // namespace li_merge

#endif // LEARNEDINDEXMERGE_IN_MEM_SSTABLE_H

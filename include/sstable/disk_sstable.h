#ifndef LEARNEDINDEXMERGE_DISK_SSTABLE_H
#define LEARNEDINDEXMERGE_DISK_SSTABLE_H

#include "in_mem_sstable.h"
#include "iterator.h"
#include "key_value_slice.h"
#include "sstable.h"
#include <assert.h>
#include <atomic>
#include <bit>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <map>
#include <math.h>
#include <stdint.h>
#include <string>
#include <thread>
#include <unistd.h>

#define PAGE_SIZE 4096
#define HEADER_SIZE 16
#define C23 1

namespace li_merge {

// TODO(chesetti): Rename FixedSizeKV to Disk.

struct DiskSSTableMetadata {
  std::string file_path;
  uint64_t num_keys;
  int key_size;
  int val_size;
  int getKVSize() { return key_size + val_size; }
  int getHeaderSize() { return 16; }
  static DiskSSTableMetadata loadMetadata(std::string file_path) {
    int fd = open(file_path.c_str(), O_RDONLY);
    char header[16];
    int bytes_read = pread(fd, header, HEADER_SIZE, 0);
    assert(bytes_read == HEADER_SIZE);
    uint64_t num_keys_ = *((uint64_t *)(header));
    int key_size_bytes_ = *((int *)(header + 8));
    int value_size_bytes_ = *((int *)(header + 12));
    return {file_path, num_keys_, key_size_bytes_, value_size_bytes_};
    close(fd);
  }
};

class DiskSSTablePageCache {
public:
  DiskSSTablePageCache(DiskSSTableMetadata metadata) : metadata_(metadata) {
    fd_ = open(metadata_.file_path.c_str(), O_RDONLY);
    buf_ = new char[PAGE_SIZE];
    buf_capacity_ = PAGE_SIZE;
    buf_first_idx_ = 0;
    buf_len_ = 0;
    disk_fetches_ = 0;
    total_bytes_fetched_ = 0;
  }
  ~DiskSSTablePageCache() {
    close(fd_);
    delete buf_;
  }
  void resize(uint64_t num_items) {
    delete buf_;
    uint64_t num_items_per_page =
        std::ceil((1.0 * PAGE_SIZE) / (metadata_.getKVSize()));
    uint64_t num_pages = std::ceil((1.0 * num_items) / num_items_per_page);
    buf_capacity_ = num_pages * PAGE_SIZE;
    buf_ = new char[buf_capacity_];
    buf_len_ = 0;
    buf_first_idx_ = 0;
  }

  bool isIdxInBuffer(uint64_t lo_idx) {
    if (!buf_len_)
      return false;
    if (buf_first_idx_ > lo_idx)
      return false;
    uint64_t buf_last_idx_ =
        buf_first_idx_ + (buf_len_) / metadata_.getKVSize();
    if (lo_idx >= buf_last_idx_)
      return false;
    return true;
  }

  void loadBufferFrom(uint64_t lo_idx) {
    disk_fetches_++;
    buf_len_ =
        pread(fd_, buf_, buf_capacity_,
              metadata_.getHeaderSize() + lo_idx * metadata_.getKVSize());
    total_bytes_fetched_ += buf_len_;
    is_buf_loaded_ = true;
    buf_first_idx_ = lo_idx;
  }

  Window<KVSlice> getWindowFrom(uint64_t lo_idx, uint64_t hi_idx) {
    uint64_t lo_page = (lo_idx * metadata_.getKVSize()) / (PAGE_SIZE);
    if (!isIdxInBuffer(lo_idx)) {
      loadBufferFrom(lo_idx);
    }
    Window<KVSlice> w;
    w.lo_idx = lo_idx;
    w.hi_idx = buf_first_idx_ + (buf_len_) / metadata_.getKVSize();
    w.hi_idx = std::min(w.hi_idx, hi_idx);
    w.buf = buf_ + ((lo_idx - buf_first_idx_) * metadata_.getKVSize());
    w.buf_len = (w.hi_idx - w.lo_idx) * metadata_.getKVSize();
    w.iter = nullptr;
    return w;
  }

  uint64_t getDiskFetches() { return disk_fetches_; }

  uint64_t getDiskFetchSize() { return buf_capacity_; }

  uint64_t getTotalBytesFetched() { return total_bytes_fetched_; }

private:
  DiskSSTableMetadata metadata_;
  int fd_;
  // Buffer holding pages.
  char *buf_;
  uint64_t buf_capacity_;
  uint64_t buf_len_;
  bool is_buf_loaded_;
  uint64_t buf_first_idx_;
  uint64_t disk_fetches_;
  uint64_t total_bytes_fetched_;
};

class DiskSSTableWindowIterator : public WindowIterator<KVSlice> {
public:
  DiskSSTableWindowIterator(DiskSSTableMetadata metadata)
      : metadata_(metadata) {
    cache_ = new DiskSSTablePageCache(metadata);
  }

  void setWindowSize(uint64_t num_keys) override { cache_->resize(num_keys); }

  Window<KVSlice> getWindow(uint64_t lo_idx, uint64_t hi_idx) override {
    return cache_->getWindowFrom(lo_idx, hi_idx);
  }
  uint64_t getDiskFetches() override { return cache_->getDiskFetches(); }
  uint64_t getDiskFetchSize() override { return cache_->getDiskFetchSize(); }
  uint64_t getTotalBytesFetched() override {
    return cache_->getTotalBytesFetched();
  }

private:
  int fd_;
  uint64_t window_size_;
  DiskSSTableMetadata metadata_;
  DiskSSTablePageCache *cache_;
};

class DiskSSTableIterator : public Iterator<KVSlice> {
public:
  DiskSSTableIterator(std::string file_path) {
    cur_idx_ = 0;
    metadata_ = DiskSSTableMetadata::loadMetadata(file_path);
    cur_kv_cache_ = new DiskSSTablePageCache(metadata_);
    peek_kv_cache_ = new DiskSSTablePageCache(metadata_);
  }
  DiskSSTableIterator(DiskSSTableMetadata metadata) : metadata_(metadata) {
    cur_idx_ = 0;
    cur_kv_cache_ = new DiskSSTablePageCache(metadata_);
    peek_kv_cache_ = new DiskSSTablePageCache(metadata_);
  }
  DiskSSTableIterator() {
    delete cur_kv_cache_;
    delete peek_kv_cache_;
  }
  bool valid() override { return cur_idx_ < metadata_.num_keys; }
  void next() override { cur_idx_++; }
  KVSlice peek(uint64_t pos) override {
    // TODO(chesetti): Assert pos is in bounds.
    peekWindow_ = peek_kv_cache_->getWindowFrom(pos, pos + 1);
    return KVSlice(peekWindow_.buf, metadata_.key_size, metadata_.val_size);
  }
  KVSlice key() override {
    curWindow_ = peek_kv_cache_->getWindowFrom(cur_idx_, cur_idx_ + 1);
    return KVSlice(curWindow_.buf, metadata_.key_size, metadata_.val_size);
  }
  void seekToFirst() override { cur_idx_ = 0; }
  void seekTo(uint64_t pos) override { cur_idx_ = pos; }
  uint64_t currentPos() override { return cur_idx_; }
  uint64_t numElts() override { return metadata_.num_keys; }
  uint64_t getDiskFetches() override {
    return cur_kv_cache_->getDiskFetches() + peek_kv_cache_->getDiskFetches();
  }
  uint64_t getTotalBytesFetched() override {
    return cur_kv_cache_->getTotalBytesFetched() +
           peek_kv_cache_->getTotalBytesFetched();
  }

  uint64_t getDiskFetchSize() override {
    return cur_kv_cache_
        ->getDiskFetchSize(); // Both peek and cur will have same fetch size.
  }

private:
  uint64_t cur_idx_; // cur_idx_ always (0, num_keys]
  DiskSSTableMetadata metadata_;
  DiskSSTablePageCache *cur_kv_cache_;
  DiskSSTablePageCache *peek_kv_cache_;
  Window<KVSlice> curWindow_;
  Window<KVSlice> peekWindow_;
};

class FixedSizeKVDiskSSTable : public SSTable<KVSlice> {
public:
  FixedSizeKVDiskSSTable(std::string file_path) {
    metadata_ = DiskSSTableMetadata::loadMetadata(file_path);
  }

  Iterator<KVSlice> *iterator() override {
    return new DiskSSTableIterator(metadata_);
  }

  WindowIterator<KVSlice> *windowIterator() {
    return new DiskSSTableWindowIterator(metadata_);
  }

  FixedSizeKVInMemSSTable *load_sstable_in_mem() {
    FixedSizeKVInMemSSTableBuilder *builder =
        FixedSizeKVInMemSSTableBuilder::InMemMalloc(
            metadata_.num_keys, metadata_.key_size, metadata_.val_size,
            nullptr);
    Iterator<KVSlice> *iter = this->iterator();
    iter->seekToFirst();
    while (iter->valid()) {
      builder->add(iter->key());
      iter->next();
    }
    FixedSizeKVInMemSSTable *in_mem_table =
        (FixedSizeKVInMemSSTable *)builder->build();
    delete iter;
    return in_mem_table;
  }

private:
  DiskSSTableMetadata metadata_;
};

// TODO(chesetti): Refactor with DiskSSTableMetadata.
class FixedSizeKVDiskSSTableBuilder : public SSTableBuilder<KVSlice> {
public:
  FixedSizeKVDiskSSTableBuilder(std::string file_path, int key_size_bytes,
                                int value_size_bytes,
                                uint64_t start_offset_idx = 0,
                                bool is_new_file = true)
      : file_path_(file_path), key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes),
        kv_size_bytes_(key_size_bytes + value_size_bytes),
        buffer_(new char[PAGE_SIZE]), buffer_offset_(0),
        buffer_size_(PAGE_SIZE),
        file_start_offset_(HEADER_SIZE + start_offset_idx * (key_size_bytes +
                                                             value_size_bytes)),
        file_cur_offset_(0), num_keys_(0), built_(false) {
    int fd_flags = O_WRONLY;
    if (is_new_file) {
      remove(file_path.c_str());
      fd_flags = fd_flags | O_CREAT;
    }
    fd_ = open(file_path.c_str(), fd_flags, 0644);
    if (fd_ == -1) {
      perror("popen");
      abort();
    }
  }
  ~FixedSizeKVDiskSSTableBuilder() { delete[] buffer_; }
  void add(const KVSlice &kv) override;
  void addWindow(const Window<KVSlice> &w) override;
  SSTable<KVSlice> *build() override {
    if (!built_) {
      flushBufferToDisk();
      writeHeader();
      close(fd_);
      built_ = true;
    }
    // fprintf(stderr, "[%ld %ld]\n", file_start_offset_, file_start_offset_ +
    // file_cur_offset_);
    return new FixedSizeKVDiskSSTable(file_path_);
  }

private:
  int fd_;
  std::string file_path_;
  int key_size_bytes_;
  int value_size_bytes_;
  int kv_size_bytes_;
  char *buffer_;
  uint64_t buffer_size_;
  uint64_t buffer_offset_;
  uint64_t file_start_offset_;
  uint64_t file_cur_offset_;
  uint64_t num_keys_;
  bool built_;
  void flushBufferToDisk();
  void writeHeader();
};

void addKeysToBuilder(Iterator<KVSlice> *iterator,
                      SSTableBuilder<KVSlice> *builder) {
  iterator->seekToFirst();
  while (iterator->valid()) {
    builder->add(iterator->key());
    iterator->next();
  }
  builder->build();
}

/*
Will write out individual SSTables in the same file for each range.
Use this if you know exactly how many elements you need for each range.
*/

class PFixedSizeKVDiskSSTableBuilder : public PSSTableBuilder<KVSlice> {
public:
  PFixedSizeKVDiskSSTableBuilder(std::string file_path, int key_size_bytes,
                                 int value_size_bytes)
      : file_path_(file_path), key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes), num_keys_(0), built_(false) {
    remove(file_path.c_str());
    int fd_flags = O_WRONLY | O_CREAT;
    int fd_ = open(file_path_.c_str(), fd_flags, 0644);
    if (fd_ < 0) {
      perror("open");
      abort();
    }
    close(fd_);
  }
  SSTableBuilder<KVSlice> *getBuilderForRange(uint64_t start_index,
                                              uint64_t end_index) override {
    num_keys_ += (end_index - start_index);
    return new FixedSizeKVDiskSSTableBuilder(
        file_path_, key_size_bytes_, value_size_bytes_, start_index, false);
  }
  SSTable<KVSlice> *build() override {
    if (!built_) {
      int fd_flags = O_WRONLY;
      int fd_ = open(file_path_.c_str(), fd_flags, 0644);
      if (fd_ < 0) {
        perror("open");
        abort();
      }
      char header[HEADER_SIZE];
      assert(sizeof(key_size_bytes_) == 4);
      assert(sizeof(num_keys_) == 8);
      memcpy(header, (char *)(&num_keys_), sizeof(num_keys_));
      memcpy(header + 8, (char *)(&key_size_bytes_), sizeof(key_size_bytes_));
      memcpy(header + 12, (char *)(&value_size_bytes_),
             sizeof(value_size_bytes_));
      uint64_t bytes_written = 0;
      while (bytes_written < HEADER_SIZE) {
        int ret =
            pwrite(fd_, header + bytes_written, HEADER_SIZE - bytes_written, 0);
        if (ret < 0) {
          perror("pwrite");
          abort();
        }
        bytes_written += ret;
      }
      close(fd_);
      built_ = true;
    }
    return new FixedSizeKVDiskSSTable(file_path_);
  }

private:
  std::string file_path_;
  int key_size_bytes_;
  int value_size_bytes_;
  std::atomic_uint64_t num_keys_;
  bool built_;
};

/* Will write out individual SSTables in new files for each range and then merge
 * them together. */
class PSplitFixedSizeKVDiskSSTableBuilder : public PSSTableBuilder<KVSlice> {
public:
  PSplitFixedSizeKVDiskSSTableBuilder(std::string file_path, int key_size_bytes,
                                      int value_size_bytes)
      : file_path_(file_path), key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes) {}
  SSTableBuilder<KVSlice> *getBuilderForRange(uint64_t start_index,
                                              uint64_t end_index) override {
    // Create a new file for each range and then merge them later.
    std::string subRangeFilePath =
        file_path_ + "_" + std::to_string(start_index);
    subRangeBuilders[start_index] = new FixedSizeKVDiskSSTableBuilder(
        subRangeFilePath, key_size_bytes_, value_size_bytes_, 0, true);
    return subRangeBuilders[start_index];
  }

  SSTable<KVSlice> *build() override {
    // TODO: Skip building if only subRange was used.
    // Just rename the file in that case.
    uint64_t num_keys_ = 0;
    std::vector<std::thread> threads;
    PSSTableBuilder<KVSlice> *pBuilder = new PFixedSizeKVDiskSSTableBuilder(
        file_path_, key_size_bytes_, value_size_bytes_);
    for (auto subRange : subRangeBuilders) {
      SSTable<KVSlice> *subRangeSrcSSTable = subRange.second->build();
      Iterator<KVSlice> *subRangeSrcIter = subRangeSrcSSTable->iterator();
      SSTableBuilder<KVSlice> *subRangeDstSSTableBuilder =
          pBuilder->getBuilderForRange(num_keys_,
                                       num_keys_ + subRangeSrcIter->numElts());
      threads.push_back(std::thread(addKeysToBuilder, subRangeSrcIter,
                                    subRangeDstSSTableBuilder));
      num_keys_ += subRangeSrcIter->numElts();
    }
    for (int i = 0; i < threads.size(); i++) {
      threads[i].join();
    }
    // Clean up the subRange files.
    for (auto subRange : subRangeBuilders) {
      std::string subRangeFilePath =
          file_path_ + "_" + std::to_string(subRange.first);
      remove(subRangeFilePath.c_str());
    }
    return pBuilder->build();
  }

private:
  std::string file_path_;
  int key_size_bytes_;
  int value_size_bytes_;
  std::map<uint64_t, SSTableBuilder<KVSlice> *> subRangeBuilders;
};
// ========= IMPLEMENTATTION ===============

void FixedSizeKVDiskSSTableBuilder::writeHeader() {
  char header[HEADER_SIZE];
  assert(sizeof(key_size_bytes_) == 4);
  assert(sizeof(num_keys_) == 8);
  memcpy(header, (char *)(&num_keys_), sizeof(num_keys_));
  memcpy(header + 8, (char *)(&key_size_bytes_), sizeof(key_size_bytes_));
  memcpy(header + 12, (char *)(&value_size_bytes_), sizeof(value_size_bytes_));
  uint64_t bytes_written = 0;
  while (bytes_written < HEADER_SIZE) {
    bytes_written +=
        pwrite(fd_, header + bytes_written, HEADER_SIZE - bytes_written, 0);
  }
}

void FixedSizeKVDiskSSTableBuilder::flushBufferToDisk() {
  uint64_t bytes_written = 0;
  while (bytes_written < buffer_offset_) {
    bytes_written +=
        pwrite(fd_, buffer_ + bytes_written, buffer_offset_ - bytes_written,
               file_start_offset_ + file_cur_offset_ + bytes_written);
  }
  buffer_offset_ = 0;
  file_cur_offset_ += bytes_written;
}

void FixedSizeKVDiskSSTableBuilder::add(const KVSlice &kvSlice) {
  assert(kvSlice.total_size_bytes() == kv_size_bytes_);
  if (buffer_offset_ == buffer_size_) {
    flushBufferToDisk();
  }
  memcpy(buffer_ + buffer_offset_, kvSlice.data(), kv_size_bytes_);
  buffer_offset_ += kv_size_bytes_;
  num_keys_++;
}

void FixedSizeKVDiskSSTableBuilder::addWindow(const Window<KVSlice> &w) {
  char *buf = w.buf;
  uint64_t bytes_to_copy = (w.hi_idx - w.lo_idx) * kv_size_bytes_;
  while (bytes_to_copy) {
    uint64_t buffer_space = buffer_size_ - buffer_offset_;
    uint64_t len = std::min(buffer_space, bytes_to_copy);
    memcpy(buffer_ + buffer_offset_, buf, len);
    buffer_offset_ += len;
    buf += len;
    num_keys_ += (len) / (kv_size_bytes_);
    if (buffer_offset_ == buffer_size_) {
      flushBufferToDisk();
    }
    bytes_to_copy -= len;
  }
}

} // namespace li_merge
#endif // LEARNEDINDEXMERGE_DISK_SSTABLE_H

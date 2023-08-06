#ifndef LEARNEDINDEXMERGE_DISK_SSTABLE_H
#define LEARNEDINDEXMERGE_DISK_SSTABLE_H

#include "file_page_cache.h"
#include "in_mem_sstable.h"
#include "iterator.h"
#include "key_value_slice.h"
#include "sstable.h"
#include <assert.h>
#include <fcntl.h>
#include <fstream>
#include <map>
#include <string>
#include <thread>
#include <unistd.h>

#define HEADER_SIZE 16

namespace li_merge {

// TODO(chesetti): Extract SSTable Header into own class.

class FixedKSizeKVFileCache {
public:
  FixedKSizeKVFileCache(int fd, int key_size_bytes, int value_size_bytes,
                        uint64_t file_start_offset)
      : file_page_cache_(new FileSinglePageCache(fd, file_start_offset)),
        key_size_bytes_(key_size_bytes), value_size_bytes_(value_size_bytes),
        kv_size_bytes_(key_size_bytes + value_size_bytes) {}
  KVSlice get_kv(uint64_t kv_idx) {
    uint64_t page_idx = (kv_idx * kv_size_bytes_) / PAGE_SIZE;
    uint64_t page_offset = (kv_idx * kv_size_bytes_) % PAGE_SIZE;
    char *page_data = file_page_cache_->get_page(page_idx);
    return KVSlice(page_data + page_offset, key_size_bytes_, value_size_bytes_);
  }

private:
  int kv_size_bytes_;
  int key_size_bytes_;
  int value_size_bytes_;
  FilePageCache *file_page_cache_;
};

class FixedSizeKVDiskSSTableIterator : public Iterator<KVSlice> {
public:
  FixedSizeKVDiskSSTableIterator(std::string file_path, uint64_t start_idx,
                                 uint64_t end_idx)
      : cur_idx_(start_idx), start_idx_(start_idx), end_idx_(end_idx) {
    fd_ = open(file_path.c_str(), O_RDONLY);
    readHeader();
    num_keys_ = end_idx - start_idx;
    cur_kv_cache_ = new FixedKSizeKVFileCache(fd_, key_size_bytes_,
                                              value_size_bytes_, HEADER_SIZE);
    peek_kv_cache_ = new FixedKSizeKVFileCache(fd_, key_size_bytes_,
                                               value_size_bytes_, HEADER_SIZE);
  }
  ~FixedSizeKVDiskSSTableIterator() {
    delete cur_kv_cache_;
    delete peek_kv_cache_;
  }
  bool valid() override { return cur_idx_ < end_idx_; }
  void next() override { cur_idx_++; }
  KVSlice peek(uint64_t pos) override {
    assert(pos + start_idx_ < end_idx_);
    return peek_kv_cache_->get_kv(pos + start_idx_);
  }
  void seekToFirst() override { cur_idx_ = start_idx_; }
  KVSlice key() override { return cur_kv_cache_->get_kv(cur_idx_); }
  uint64_t current_pos() override { return cur_idx_ - start_idx_; }
  uint64_t num_elts() override { return num_keys_; }

private:
  int fd_;
  uint64_t cur_idx_; // cur_idx_ always (start_idx_, end_idx_]
  uint64_t start_idx_;
  uint64_t end_idx_;
  uint64_t num_keys_;    // total number of keys in this iterator.
  int key_size_bytes_;   // loaded from header
  int value_size_bytes_; // loaded from header
  int kv_size_bytes_;
  FixedKSizeKVFileCache *cur_kv_cache_;
  FixedKSizeKVFileCache *peek_kv_cache_;
  void readHeader();
};

class FixedSizeKVDiskSSTable : public SSTable<KVSlice> {
public:
  FixedSizeKVDiskSSTable(std::string file_path) : file_path_(file_path) {
    readHeader();
    start_idx_ = 0;
    end_idx_ = num_keys_;
  }
  FixedSizeKVDiskSSTable(std::string file_path, uint64_t start, uint64_t end)
      : file_path_(file_path) {
    readHeader();
    num_keys_ = end - start;
    start_idx_ = start;
    end_idx_ = end;
  }
  Iterator<KVSlice> *iterator() override {
    return new FixedSizeKVDiskSSTableIterator(file_path_, start_idx_, end_idx_);
  }
  FixedSizeKVInMemSSTable *load_sstable_in_mem() {
    FixedSizeKVInMemSSTableBuilder *builder =
        new FixedSizeKVInMemSSTableBuilder(0, key_size_bytes_,
                                           value_size_bytes_, nullptr);
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
  SSTable<KVSlice> *getSSTableForSubRange(uint64_t start,
                                          uint64_t end) override {
    return new FixedSizeKVDiskSSTable(file_path_, start, end);
  }

private:
  uint64_t num_keys_;
  int key_size_bytes_;
  int value_size_bytes_;
  uint64_t start_idx_;
  uint64_t end_idx_;
  std::string file_path_;
  void readHeader();
};

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
  SSTable<KVSlice> *build() override {
    if (!built_) {
      flushBufferToDisk();
      writeHeader();
      close(fd_);
      built_ = true;
    }
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

void *addKeysToBuilder(Iterator<KVSlice> *iterator,
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
  uint64_t num_keys_;
  bool built_;
};

/* Will write out individual SSTables in new files for each range and then merge them together. */
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
    uint64_t num_keys_ = 0;
    std::vector<std::thread> threads;
    PSSTableBuilder<KVSlice> *pBuilder = new PFixedSizeKVDiskSSTableBuilder(
        file_path_, key_size_bytes_, value_size_bytes_);
    for (auto subRange : subRangeBuilders) {
      SSTable<KVSlice> *subRangeSrcSSTable = subRange.second->build();
      Iterator<KVSlice> *subRangeSrcIter = subRangeSrcSSTable->iterator();
      SSTableBuilder<KVSlice> *subRangeDstSSTableBuilder =
          pBuilder->getBuilderForRange(num_keys_,
                                       num_keys_ + subRangeSrcIter->num_elts());
      threads.push_back(std::thread(addKeysToBuilder, subRangeSrcIter,
                                    subRangeDstSSTableBuilder));
      num_keys_ += subRangeSrcIter->num_elts();
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

void FixedSizeKVDiskSSTableIterator::readHeader() {
  char header[16];
  uint64_t bytes_read = 0;
  while (bytes_read < HEADER_SIZE) {
    bytes_read += pread(fd_, header, HEADER_SIZE - bytes_read, bytes_read);
  }
  num_keys_ = *((uint64_t *)(header));
  key_size_bytes_ = *((int *)(header + 8));
  value_size_bytes_ = *((int *)(header + 12));
  kv_size_bytes_ = key_size_bytes_ + value_size_bytes_;
}

void FixedSizeKVDiskSSTable::readHeader() {
  int fd = open(file_path_.c_str(), O_RDONLY);
  char header[16];
  uint64_t bytes_read = 0;
  while (bytes_read < HEADER_SIZE) {
    bytes_read += pread(fd, header, HEADER_SIZE - bytes_read, bytes_read);
  }
  num_keys_ = *((uint64_t *)(header));
  key_size_bytes_ = *((int *)(header + 8));
  value_size_bytes_ = *((int *)(header + 12));
  close(fd);
}

} // namespace li_merge
#endif // LEARNEDINDEXMERGE_DISK_SSTABLE_H

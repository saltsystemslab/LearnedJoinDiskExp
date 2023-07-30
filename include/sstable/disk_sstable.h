#ifndef LEARNEDINDEXMERGE_DISK_SSTABLE_H
#define LEARNEDINDEXMERGE_DISK_SSTABLE_H

#include "file_page_cache.h"
#include "in_mem_sstable.h"
#include "iterator.h"
#include "key_value_slice.h"
#include "sstable.h"
#include <assert.h>
#include <fcntl.h>
#include <string>
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
  FixedSizeKVDiskSSTableIterator(std::string file_path_) : cur_idx_(0) {
    fd_ = open(file_path_.c_str(), O_RDONLY);
    readHeader();
    cur_kv_cache_ = new FixedKSizeKVFileCache(fd_, key_size_bytes_,
                                              value_size_bytes_, HEADER_SIZE);
    peek_kv_cache_ = new FixedKSizeKVFileCache(fd_, key_size_bytes_,
                                               value_size_bytes_, HEADER_SIZE);
  }
  ~FixedSizeKVDiskSSTableIterator() {
    delete cur_kv_cache_;
    delete peek_kv_cache_;
  }
  bool valid() override { return cur_idx_ < num_keys_; }
  void next() override { cur_idx_++; }
  KVSlice peek(uint64_t pos) override {
    assert(pos < num_keys_);
    return peek_kv_cache_->get_kv(pos);
  }
  void seekToFirst() override { cur_idx_ = 0; }
  KVSlice key() override { return cur_kv_cache_->get_kv(cur_idx_); }
  uint64_t current_pos() override { return cur_idx_; }
  uint64_t num_elts() override { return num_keys_; }

private:
  int fd_;
  uint64_t cur_idx_;
  uint64_t num_keys_;
  int key_size_bytes_;
  int value_size_bytes_;
  int kv_size_bytes_;
  FixedKSizeKVFileCache *cur_kv_cache_;
  FixedKSizeKVFileCache *peek_kv_cache_;
  void readHeader();
};

class FixedSizeKVDiskSSTable : public SSTable<KVSlice> {
public:
  FixedSizeKVDiskSSTable(std::string file_path) : file_path_(file_path) {
    readHeader();
  }
  Iterator<KVSlice> *iterator() override {
    return new FixedSizeKVDiskSSTableIterator(file_path_);
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

private:
  uint64_t num_keys_;
  int key_size_bytes_;
  int value_size_bytes_;
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
        file_cur_offset_(0), num_keys_(0) {
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
    flushBufferToDisk();
    writeHeader();
    close(fd_);
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
  void flushBufferToDisk();
  void writeHeader();
};

class PFixedSizeKVDiskSSTableBuilder : public PSSTableBuilder<KVSlice> {
public:
  PFixedSizeKVDiskSSTableBuilder(std::string file_path, int key_size_bytes,
                                 int value_size_bytes, uint64_t num_keys)
      : file_path_(file_path), key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes), num_keys_(num_keys) {
    remove(file_path.c_str());
    int fd_flags = O_WRONLY | O_CREAT;
    fd_ = open(file_path.c_str(), fd_flags, 0644);
  }
  SSTableBuilder<KVSlice> *getBuilderForRange(uint64_t offset_index) override {
    return new FixedSizeKVDiskSSTableBuilder(file_path_, key_size_bytes_,
                                             value_size_bytes_, offset_index, false);
  }
  SSTable<KVSlice> *build() override {
    char header[HEADER_SIZE];
    assert(sizeof(key_size_bytes_) == 4);
    assert(sizeof(num_keys_) == 8);
    memcpy(header, (char *)(&num_keys_), sizeof(num_keys_));
    memcpy(header + 8, (char *)(&key_size_bytes_), sizeof(key_size_bytes_));
    memcpy(header + 12, (char *)(&value_size_bytes_),
           sizeof(value_size_bytes_));
    uint64_t bytes_written = 0;
    while (bytes_written < HEADER_SIZE) {
      bytes_written +=
          pwrite(fd_, header + bytes_written, HEADER_SIZE - bytes_written, 0);
    }
    close(fd_);
    return new FixedSizeKVDiskSSTable(file_path_);
  }

private:
  int fd_;
  std::string file_path_;
  int key_size_bytes_;
  int value_size_bytes_;
  uint64_t num_keys_;
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

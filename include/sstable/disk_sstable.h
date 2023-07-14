#ifndef LEARNEDINDEXMERGE_DISK_SSTABLE_H
#define LEARNEDINDEXMERGE_DISK_SSTABLE_H

#include "iterator.h"
#include "key_value_slice.h"
#include "sstable.h"
#include <assert.h>
#include <fcntl.h>
#include <string>
#include <unistd.h>

// 8 bytes num_keys, 4 bytes key_size, 4 bytes value size
#define HEADER_SIZE 16
#define BUFFER_SIZE 4096

namespace li_merge {

class DiskSingleBlockCache {
public:
  DiskSingleBlockCache(int fd, int kv_size_bytes,
                       uint64_t file_block_size_bytes,
                       uint64_t file_start_offset)
      : buffer_(new char[file_block_size_bytes]), fd_(fd),
        kv_size_bytes_(kv_size_bytes),
        file_block_size_bytes_(file_block_size_bytes), buffer_loaded_(false),
        file_start_offset_(file_start_offset) {
    if (file_block_size_bytes % kv_size_bytes != 0) {
      fprintf(stderr, "KV size not aligned with Block Size");
      abort();
    }
  }
  ~DiskSingleBlockCache() { delete[] buffer_; }
  char *read(uint64_t kv_idx);

private:
  int fd_;
  char *buffer_;
  bool buffer_loaded_;
  uint64_t file_block_size_bytes_;
  uint64_t file_start_offset_;
  int kv_size_bytes_;
  uint64_t cur_block_idx_;
  void loadBlock(uint64_t block_idx);
};

class FixedSizeKVDiskSSTableIterator : public Iterator<KVSlice> {
public:
  FixedSizeKVDiskSSTableIterator(std::string file_path_) : curBuffer_(0) {
    fd_ = open(file_path_.c_str(), O_RDONLY);
    readHeader();
    curBuffer_ =
        new DiskSingleBlockCache(fd_, kv_size_bytes_, BUFFER_SIZE, HEADER_SIZE);
    peekBuffer_ =
        new DiskSingleBlockCache(fd_, kv_size_bytes_, BUFFER_SIZE, HEADER_SIZE);
  }
  bool valid() override { return cur_idx_ < num_keys_; }
  void next() override { cur_idx_++; }
  KVSlice peek(uint64_t pos) override {
    return KVSlice(peekBuffer_->read(pos), key_size_bytes_, value_size_bytes_);
  }
  void seekToFirst() override { cur_idx_ = 0; }
  KVSlice key() override {
    return KVSlice(curBuffer_->read(cur_idx_), key_size_bytes_,
                   value_size_bytes_);
  }
  uint64_t current_pos() override { return cur_idx_; }
  uint64_t num_elts() override { return num_keys_; }

private:
  int fd_;
  uint64_t cur_idx_;
  uint64_t num_keys_;
  int key_size_bytes_;
  int value_size_bytes_;
  int kv_size_bytes_;
  DiskSingleBlockCache *curBuffer_;
  DiskSingleBlockCache *peekBuffer_;
  void readHeader();
};

class FixedSizeKVDiskSSTable : public SSTable<KVSlice> {
public:
  FixedSizeKVDiskSSTable(std::string file_path) : file_path_(file_path) {}
  Iterator<KVSlice> *iterator() override {
    return new FixedSizeKVDiskSSTableIterator(file_path_);
  }

private:
  std::string file_path_;
};

class FixedSizeKVDiskSSTableBuilder : public SSTableBuilder<KVSlice> {
public:
  FixedSizeKVDiskSSTableBuilder(std::string file_path, int key_size_bytes,
                                int value_size_bytes,
                                uint64_t buffer_size = BUFFER_SIZE)
      : file_path_(file_path), key_size_bytes_(key_size_bytes),
        value_size_bytes_(value_size_bytes),
        kv_size_bytes_(key_size_bytes + value_size_bytes),
        buffer_(new char[buffer_size]), buffer_offset_(0),
        buffer_size_(buffer_size), file_start_offset_(HEADER_SIZE),
        file_cur_offset_(0), num_keys_(0) {
    remove(file_path.c_str());
    fd_ = open(file_path.c_str(), O_WRONLY | O_CREAT, 0644);
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

// ========= IMPLEMENTATTION ===============

char *DiskSingleBlockCache::read(uint64_t kv_idx) {
  uint64_t block_idx = (kv_idx * kv_size_bytes_) / (file_block_size_bytes_);
  uint64_t block_offset = (kv_idx * kv_size_bytes_) % (file_block_size_bytes_);
  if (!buffer_loaded_ || block_idx != cur_block_idx_) {
    loadBlock(cur_block_idx_);
  }
  return buffer_ + block_offset;
}

void DiskSingleBlockCache::loadBlock(uint64_t block_idx) {
  uint64_t bytes_read = 0;
  while (bytes_read < file_block_size_bytes_) {
    bytes_read += pread(
        fd_, buffer_ + bytes_read, file_block_size_bytes_ - bytes_read,
        file_start_offset_ + block_idx * file_block_size_bytes_ + bytes_read);
  }
  cur_block_idx_ = block_idx;
  buffer_loaded_ = true;
}

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

} // namespace li_merge
#endif // LEARNEDINDEXMERGE_DISK_SSTABLE_H

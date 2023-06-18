#ifndef SLICE_FILE_ITERATOR_H
#define SLICE_FILE_ITERATOR_H

#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

#include "file_block_key_cache.h"
#include "iterator.h"
#include "slice.h"

class SliceFileIterator : public Iterator<Slice> {
public:
  SliceFileIterator(int file_descriptor, uint64_t n, uint32_t key_size,
                    std::string id)
      : file_descriptor_(file_descriptor), key_size_(key_size), cur_idx_(0),
        cur_key_buffer_(new FileKeyBlock(file_descriptor, PAGE_SIZE, key_size)),
        peek_key_buffer_(
            new FileKeyBlock(file_descriptor, PAGE_SIZE, key_size)),
        id_(id), num_keys_(n), start_offset_idx_(0) {}

  SliceFileIterator(int file_descriptor, uint64_t n, uint64_t start_offset_idx,
                    uint32_t key_size, std::string id)
      : file_descriptor_(file_descriptor), key_size_(key_size), cur_idx_(0),
        cur_key_buffer_(new FileKeyBlock(file_descriptor, PAGE_SIZE, key_size)),
        peek_key_buffer_(
            new FileKeyBlock(file_descriptor, PAGE_SIZE, key_size)),
        id_(id), num_keys_(n), start_offset_idx_(start_offset_idx) {}

  ~SliceFileIterator() {
    delete cur_key_buffer_;
    delete peek_key_buffer_;
  }
  bool valid() const override;
  void next() override;
  Slice key() override;
  Slice peek(uint64_t pos) const override;
  void seekToFirst() override;
  uint64_t current_pos() const override;
  uint64_t bulkReadAndForward(uint64_t num_keys, char **data,
                              uint64_t *len) override;

  std::string id() override { return id_; }
  uint64_t num_keys() const override { return num_keys_; }
  Iterator<Slice> *subRange(uint64_t start, uint64_t end) override {
    return new SliceFileIterator(file_descriptor_, end - start, start,
                                 key_size_, id_);
  }

private:
  int file_descriptor_;
  uint64_t cur_idx_;
  uint64_t start_offset_idx_;
  uint64_t num_keys_;
  int key_size_;
  FileKeyBlock *cur_key_buffer_;
  FileKeyBlock *peek_key_buffer_;
  std::string id_;
};

class SliceFileIteratorBuilder : public IteratorBuilder<Slice> {
public:
  SliceFileIteratorBuilder(const char *file_name, size_t buffer_size,
                           int key_size, std::string id)
      : file_name_(new char[strlen(file_name) + 1]), buffer_size_(buffer_size),
        key_size_(key_size), num_keys_(0), buffer_idx_(0), id_(id),
        file_offset_(0) {
    memcpy(file_name_, file_name, strlen(file_name));
    file_name_[strlen(file_name)] = '\0';
    file_descriptor_ = open(file_name_, O_WRONLY | O_CREAT, 0644);
    if (file_descriptor_ == -1) {
      perror("popen");
      abort();
    }
    buffer_ = new char[buffer_size_];
  }

  SliceFileIteratorBuilder(const char *file_name, size_t buffer_size,
                           uint64_t start_idx_offset, int key_size,
                           std::string id)
      : file_name_(new char[strlen(file_name) + 1]), buffer_size_(buffer_size),
        key_size_(key_size), num_keys_(0), buffer_idx_(0), id_(id),
        file_offset_(start_idx_offset * key_size) {
    memcpy(file_name_, file_name, strlen(file_name));
    file_name_[strlen(file_name)] = '\0';
    file_descriptor_ = open(file_name_, O_WRONLY | O_CREAT, 0644);
    if (file_descriptor_ == -1) {
      perror("popen");
      abort();
    }
    buffer_ = new char[buffer_size_];
  }

  ~SliceFileIteratorBuilder() {
    delete buffer_;
    delete file_name_;
  }

  void add(const Slice &key) override;
  Iterator<Slice> *build() override;
  void bulkAdd(Iterator<Slice> *iter, uint64_t num_keys) override { abort(); };
  IteratorBuilder<Slice> *subRange(uint64_t start, uint64_t end) {
    return new SliceFileIteratorBuilder(file_name_, buffer_size_, start,
                                        key_size_, id_);
  }

private:
  void addKeyToBuffer(const Slice &key);
  void flushBufferToDisk();
  char *file_name_;
  int file_descriptor_;
  char *buffer_;
  int key_size_;
  size_t buffer_idx_;
  size_t buffer_size_;
  off_t file_offset_;
  uint64_t num_keys_;
  std::string id_;
};

void SliceFileIteratorBuilder::add(const Slice &key) {
  num_keys_++;
  if (key.size_ + buffer_idx_ < buffer_size_) {
    addKeyToBuffer(key);
    return;
  }
  flushBufferToDisk();
  addKeyToBuffer(key);
}
bool SliceFileIterator::valid() const { return cur_idx_ < num_keys_; }

void SliceFileIterator::next() { cur_idx_++; }

Slice SliceFileIterator::key() {
  return Slice(cur_key_buffer_->read(cur_idx_ + start_offset_idx_), key_size_);
}

Slice SliceFileIterator::peek(uint64_t pos) const {
  return Slice(peek_key_buffer_->read(pos + start_offset_idx_), key_size_);
}

void SliceFileIterator::seekToFirst() { cur_idx_ = 0; }

uint64_t SliceFileIterator::bulkReadAndForward(uint64_t keys_to_copy,
                                               char **data, uint64_t *len) {
  uint64_t keys_in_buffer;
  cur_key_buffer_->pointer_to_current_block(cur_idx_, data, &keys_in_buffer);
  keys_to_copy = std::min(keys_to_copy, keys_in_buffer);
  keys_to_copy = std::min(keys_to_copy, num_keys_ - cur_idx_);
  cur_idx_ += keys_to_copy;
  *len = (keys_to_copy * key_size_);
  return keys_to_copy;
}

uint64_t SliceFileIterator::current_pos() const { return cur_idx_; }

Iterator<Slice> *SliceFileIteratorBuilder::build() {
  flushBufferToDisk();
  int read_only_fd = open(file_name_, O_RDONLY);
  if (read_only_fd == -1) {
    perror("popen");
    abort();
  }
  return new SliceFileIterator(read_only_fd, num_keys_, key_size_, id_);
}

void SliceFileIteratorBuilder::flushBufferToDisk() {
  ssize_t bytes_written =
      pwrite(file_descriptor_, buffer_, buffer_idx_, file_offset_);
  if (bytes_written == -1) {
    perror("pwrite");
    abort();
  }
  file_offset_ += buffer_idx_;
  buffer_idx_ = 0;
}

void SliceFileIteratorBuilder::addKeyToBuffer(const Slice &key) {
  assert(key.size_ <= buffer_size_);
  memcpy(buffer_ + buffer_idx_, key.data_, key.size_);
  buffer_idx_ += key.size_;
}

#if 0
void SliceFileIteratorBuilder::bulkAdd(Iterator<Slice> *iter,
                                                int keys_to_add) {
  char *data;
  uint64_t len;
  while (keys_to_add > 0) {
    uint64_t keys_added = iter->bulkReadAndForward(keys_to_add, &data, &len);
    keys_to_add -= keys_added;
    num_keys_ += keys_added;

    ssize_t bytes_written = pwrite(file_descriptor_, data, len, file_offset_);
    if (bytes_written == -1) {
      perror("pwrite");
      abort();
    }
    file_offset_ += bytes_written;
  }
}
#endif
#endif

#ifndef SLICE_FILE_ITERATOR_H
#define SLICE_FILE_ITERATOR_H

#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

#include "iterator.h"
#include "slice.h"

class SliceFileIterator : public Iterator<Slice> {
public:
  SliceFileIterator(int file_descriptor, uint64_t n, int key_size,
                             std::string id)
      : file_descriptor_(file_descriptor), key_size_(key_size),
        bulk_key_buffer_(new char[key_size * MAX_KEYS_TO_BULK_COPY]),
        cur_key_buffer_(new char[key_size]),
        peek_key_buffer_(new char[key_size]), cur_key_loaded_(false), id_(id),
        num_keys_(n) {}

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
    abort();
  }

private:
  int file_descriptor_;
  uint64_t cur_idx_;
  uint64_t num_keys_;
  int key_size_;
  char *cur_key_buffer_;
  char *peek_key_buffer_;
  char *bulk_key_buffer_;
  bool cur_key_loaded_;
  std::string id_;
};

class SliceFileIteratorBuilder : public IteratorBuilder<Slice> {
public:
  SliceFileIteratorBuilder(const char *file_name, size_t buffer_size,
                                    int key_size, int index)
      : file_name_(new char[strlen(file_name) + 1]), buffer_size_(buffer_size),
        key_size_(key_size), num_keys_(0), buffer_idx_(0), index_(index),
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

  ~SliceFileIteratorBuilder() {
    delete buffer_;
    delete file_name_;
  }

  void add(const Slice &key) override;
  Iterator<Slice> *build() override;
  void bulkAdd(Iterator<Slice> *iter, uint64_t num_keys) override {
    abort();
  };
  IteratorBuilder<Slice> *subRange(uint64_t start, uint64_t end) {
    abort();
    return nullptr;
  }

protected:
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
  int index_;
};

void SliceFileIteratorBuilder::add(const Slice &key) {
  assert(key.size_ == key_size_);
  num_keys_++;
  if (key.size_ + buffer_idx_ < buffer_size_) {
    addKeyToBuffer(key);
    return;
  }
  flushBufferToDisk();
  addKeyToBuffer(key);
}
bool SliceFileIterator::valid() const { return cur_idx_ < num_keys_; }

void SliceFileIterator::next() {
  assert(valid());
  cur_idx_++;
  cur_key_loaded_ = false;
}

Slice SliceFileIterator::key() {
  if (!cur_key_loaded_) {
    ssize_t bytes_read = pread(file_descriptor_, cur_key_buffer_, key_size_,
                               cur_idx_ * key_size_);
    if (bytes_read == -1) {
      perror("pread");
      abort();
    }
    cur_key_loaded_ = true;
  }
  return Slice(cur_key_buffer_, key_size_);
}

Slice SliceFileIterator::peek(uint64_t pos) const {
  pos = std::min(num_keys_, pos);
  ssize_t bytes_read =
      pread(file_descriptor_, peek_key_buffer_, key_size_, pos * key_size_);
  if (bytes_read == -1) {
    perror("pread");
    abort();
  }
  return Slice(peek_key_buffer_, key_size_);
}

void SliceFileIterator::seekToFirst() {
  cur_idx_ = 0;
  cur_key_loaded_ = false;
}

uint64_t SliceFileIterator::bulkReadAndForward(uint64_t keys_to_copy,
                                                        char **data,
                                                        uint64_t *len) {
  keys_to_copy = std::min(keys_to_copy, (uint64_t)MAX_KEYS_TO_BULK_COPY);
  keys_to_copy = std::min(keys_to_copy, num_keys_ - cur_idx_);
  ssize_t bytes_read = pread(file_descriptor_, bulk_key_buffer_,
                             keys_to_copy * key_size_, cur_idx_ * key_size_);
  cur_idx_ += keys_to_copy;
  cur_key_loaded_ = false;
  if (bytes_read == -1) {
    perror("pread");
    abort();
  }
  *len = bytes_read;
  *data = bulk_key_buffer_;
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
  std::string iterator_id = "identifier_" + std::to_string(index_);
  printf("Num Keys: %ld\n", num_keys_);
  return new SliceFileIterator(read_only_fd, num_keys_, key_size_,
                                        iterator_id);
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

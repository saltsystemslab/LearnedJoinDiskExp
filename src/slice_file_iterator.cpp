#include "slice_file_iterator.h"

#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <iostream>

#include "config.h"

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

void SliceFileIterator::seek(Slice item) { abort(); }
uint64_t SliceFileIterator::current_pos() const { return cur_idx_; }

Iterator<Slice> *SliceFileIteratorBuilder::finish() {
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

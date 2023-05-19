#include "slice_file_iterator.h"
#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>

void FixedSizeSliceFileIteratorBuilder::add(const Slice &key) {
  assert(key.size_ == key_size_);
  num_keys_++;
  if (key.size_ + buffer_idx_ < buffer_size_) {
    addKeyToBuffer(key);
    return;
  }
  flushBufferToDisk();
  addKeyToBuffer(key);
}
bool FixedSizeSliceFileIterator::valid() const {}
void FixedSizeSliceFileIterator::next() {}
Slice FixedSizeSliceFileIterator::key() const {}
Slice FixedSizeSliceFileIterator::peek(uint64_t pos) const {}
void FixedSizeSliceFileIterator::seekToFirst() {}
void FixedSizeSliceFileIterator::seek(Slice item) {}
uint64_t FixedSizeSliceFileIterator::current_pos() const {}

Iterator<Slice> *FixedSizeSliceFileIteratorBuilder::finish() {
  flushBufferToDisk();
  int read_only_fd = open(file_name_, O_RDONLY);
  if (read_only_fd == -1) {
    perror("popen");
    abort();
  }
  return new FixedSizeSliceFileIterator(read_only_fd, num_keys_, key_size_);
}

void FixedSizeSliceFileIteratorBuilder::flushBufferToDisk() {
  ssize_t bytes_written =
      pwrite(file_descriptor_, buffer_, buffer_idx_, file_offset_);
  if (bytes_written == -1) {
    perror("pwrite");
    abort();
  }
  file_offset_ += buffer_idx_;
  buffer_idx_ = 0;
}

void FixedSizeSliceFileIteratorBuilder::addKeyToBuffer(const Slice &key) {
  assert(key.size_ <= buffer_size_);
  memcpy(buffer_ + buffer_idx_, key.data_, key.size_);
  buffer_idx_ += key.size_;
}

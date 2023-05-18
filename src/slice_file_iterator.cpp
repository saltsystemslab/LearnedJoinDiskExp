#include "slice_file_iterator.h"
#include <cassert>
#include <cstring>
#include <unistd.h>

void SliceFileIteratorBuilder::add(const Slice &key) {
  if (key.size_ + buffer_idx_ < buffer_size_) {
    addKeyToBuffer(key);
    return;
  }
  flushBufferToDisk();
  addKeyToBuffer(key);
}

Iterator<Slice> *SliceFileIteratorBuilder::finish() {
  flushBufferToDisk();
  return nullptr;
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

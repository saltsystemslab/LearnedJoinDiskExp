#include "slice_file_iterator.h"

#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cstring>
#include <iostream>

void FixedSizeSliceFileIteratorBuilder::add(const Slice &key) {
  assert(key.size_ == key_size_);
#if LEARNED_MERGE
  plrBuilder->processKey(LdbKeyToInteger(key));
#endif
  num_keys_++;
  if (key.size_ + buffer_idx_ < buffer_size_) {
    addKeyToBuffer(key);
    return;
  }
  flushBufferToDisk();
  addKeyToBuffer(key);
}
bool FixedSizeSliceFileIterator::valid() const { return cur_idx_ < num_keys_; }

void FixedSizeSliceFileIterator::next() {
  assert(valid());
  cur_idx_++;
  cur_key_loaded_ = false;
}

Slice FixedSizeSliceFileIterator::key() {
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

Slice FixedSizeSliceFileIterator::peek(uint64_t pos) const {
  pos = std::min(num_keys_, pos);
  ssize_t bytes_read =
      pread(file_descriptor_, peek_key_buffer_, key_size_, pos * key_size_);
  if (bytes_read == -1) {
    perror("pread");
    abort();
  }
  return Slice(peek_key_buffer_, key_size_);
}

void FixedSizeSliceFileIterator::seekToFirst() {
  cur_idx_ = 0;
  cur_key_loaded_ = false;
}

void FixedSizeSliceFileIterator::seek(Slice item) { abort(); }
uint64_t FixedSizeSliceFileIterator::current_pos() const { return cur_idx_; }

Iterator<Slice> *FixedSizeSliceFileIteratorBuilder::finish() {
  flushBufferToDisk();
  int read_only_fd = open(file_name_, O_RDONLY);
  if (read_only_fd == -1) {
    perror("popen");
    abort();
  }
#if LEARNED_MERGE
  return new FixedSizeSliceFileIterator(read_only_fd, num_keys_, key_size_,
                                        plrBuilder->finishTraining());
#else
  return new FixedSizeSliceFileIterator(read_only_fd, num_keys_, key_size_, nullptr);
#endif
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

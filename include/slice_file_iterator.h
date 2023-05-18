#ifndef SLICE_FILE_ITERATOR_H
#define SLICE_FILE_ITERATOR_H

#include "iterator.h"
#include "slice.h"
#include <fcntl.h>

class SliceFileIterator : Iterator<Slice> {};

class SliceFileIteratorBuilder : public IteratorBuilder<Slice> {
public:
  SliceFileIteratorBuilder(const char *file_name, size_t buffer_size)
      : buffer_size_(buffer_size), buffer_idx_(0), file_offset_(0) {

    file_descriptor_ = open(file_name, O_WRONLY | O_CREAT, 0644);
    if (file_descriptor_ == -1) {
      perror("popen");
      abort();
    }
    buffer_ = new char[buffer_size_];
  }

  ~SliceFileIteratorBuilder() { delete buffer_; }

  void add(const Slice &key) override;
  Iterator<Slice> *finish() override;

private:
  void addKeyToBuffer(const Slice &key);
  void flushBufferToDisk();
  int file_descriptor_;
  char *buffer_;
  size_t buffer_idx_;
  size_t buffer_size_;
  off_t file_offset_;
};

#endif

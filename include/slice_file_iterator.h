#ifndef SLICE_FILE_ITERATOR_H
#define SLICE_FILE_ITERATOR_H

#include "iterator.h"
#include "slice.h"
#include <cstring>
#include <fcntl.h>

class FixedSizeSliceFileIterator : public Iterator<Slice> {
public:
<<<<<<< HEAD
  FixedSizeSliceFileIterator(int file_descriptor, uint64_t num_keys,
                             int key_size)
      : file_descriptor_(file_descriptor), num_keys_(num_keys),
        key_size_(key_size) {}

  bool valid() const override;
  void next() override;
  Slice key() const override;
  Slice peek(uint64_t pos) const override;
  void seekToFirst() override;
  void seek(Slice item) override;
  uint64_t current_pos() const override;

private:
  int file_descriptor_;
  uint64_t num_keys_;
  uint64_t cur_idx_;
  int key_size_;
};

class FixedSizeSliceFileIteratorBuilder : public IteratorBuilder<Slice> {
public:
  FixedSizeSliceFileIteratorBuilder(const char *file_name, size_t buffer_size,
                                    int key_size)
      : file_name_(new char[strlen(file_name)]), buffer_size_(buffer_size),
        key_size_(key_size), buffer_idx_(0), file_offset_(0) {
    memcpy(file_name_, file_name, strlen(file_name));
    file_descriptor_ = open(file_name_, O_WRONLY | O_CREAT, 0644);
    if (file_descriptor_ == -1) {
      perror("popen");
      abort();
    }
    buffer_ = new char[buffer_size_];
  }

  ~FixedSizeSliceFileIteratorBuilder() {
    delete buffer_;
    delete file_name_;
  }

  void add(const Slice &key) override;
  Iterator<Slice> *finish() override;

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
};

#endif

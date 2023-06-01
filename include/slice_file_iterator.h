#ifndef SLICE_FILE_ITERATOR_H
#define SLICE_FILE_ITERATOR_H

#include <fcntl.h>
#include <unistd.h>

#include <cstring>
#include <iostream>

#include "iterator.h"
#include "slice.h"
#include "slice_iterator.h"

class FixedSizeSliceFileIterator : public Iterator<Slice> {
public:
  FixedSizeSliceFileIterator(int file_descriptor, uint64_t n, int key_size,
                             std::string id)
      : file_descriptor_(file_descriptor), key_size_(key_size),
        bulk_key_buffer_(new char[key_size * MAX_KEYS_TO_BULK_COPY]),
        cur_key_buffer_(new char[key_size]),
        peek_key_buffer_(new char[key_size]), cur_key_loaded_(false), id_(id),
        num_keys_(n) {}

  ~FixedSizeSliceFileIterator() {
    delete cur_key_buffer_;
    delete peek_key_buffer_;
  }
  bool valid() const override;
  void next() override;
  Slice key() override;
  Slice peek(uint64_t pos) const override;
  void seekToFirst() override;
  void seek(Slice item) override;
  uint64_t current_pos() const override;
  uint64_t bulkReadAndForward(uint64_t num_keys, char **data,
                              uint64_t *len) override;

  std::string identifier() override { return id_; }
  uint64_t num_keys() const override { return num_keys_; }

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

class FixedSizeSliceFileIteratorBuilder : public IteratorBuilder<Slice> {
public:
  FixedSizeSliceFileIteratorBuilder(const char *file_name, size_t buffer_size,
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

  ~FixedSizeSliceFileIteratorBuilder() {
    delete buffer_;
    delete file_name_;
  }

  void add(const Slice &key) override;
  Iterator<Slice> *finish() override;
  virtual void bulkAdd(Iterator<Slice> *iter, int num_keys) override;

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

class FixedSizeSliceFileIteratorWithModelBuilder
    : public FixedSizeSliceFileIteratorBuilder {
public:
  FixedSizeSliceFileIteratorWithModelBuilder(const char *file_name,
                                             size_t buffer_size, int key_size,
                                             int index)
      : FixedSizeSliceFileIteratorBuilder(file_name, buffer_size, key_size,
                                          index),
        plrBuilder_(new PLRBuilder(PLR_ERROR_BOUND)) {}

  void add(const Slice &key) override {
    FixedSizeSliceFileIteratorBuilder::add(key);
#if USE_STRING_KEYS
    plrBuilder_->processKey(LdbKeyToInteger(key));
#else
    plrBuilder_->processKey(*(KEY_TYPE *)(key.data_));
#endif
  };
  Iterator<Slice> *finish() override {
    return new SliceIteratorWithModel(
        FixedSizeSliceFileIteratorBuilder::finish(),
        plrBuilder_->finishTraining());
  };
  virtual void bulkAdd(Iterator<Slice> *iter, int num_keys) override;

private:
  PLRBuilder *plrBuilder_;
};

#endif

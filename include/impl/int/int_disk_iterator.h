#ifndef INT_DISK_H
#define INT_DISK_H

#include "iterator.h"
#include "file_block_key_cache.h"
#define PAGE_SIZE 4096

template <class T>
class IntDiskIterator : public Iterator<T> {
 public:
  IntDiskIterator(int file_descriptor, uint64_t num_keys, std::string id)
      : file_descriptor_(file_descriptor),
        cur_key_block_(new FileKeyBlock(file_descriptor, 4096, sizeof(T))),
        peek_key_block_(new FileKeyBlock(file_descriptor, 4096, sizeof(T))),
        cur_idx_(0),
        id_(id),
        num_keys_(num_keys),
        start_key_idx_offest_(0) {}

  IntDiskIterator(int file_descriptor, uint64_t start_key_idx_offest,
                  uint64_t num_keys, std::string id)
      : file_descriptor_(file_descriptor),
        cur_key_block_(new FileKeyBlock(file_descriptor, 4096, sizeof(T))),
        peek_key_block_(new FileKeyBlock(file_descriptor, 4096, sizeof(T))),
        cur_idx_(0),
        id_(id),
        num_keys_(num_keys),
        start_key_idx_offest_(start_key_idx_offest) {
        }

  ~IntDiskIterator() {}
  bool valid() const override { return cur_idx_ < num_keys_; }
  void next() override {
    cur_idx_++;
  }
  T peek(uint64_t pos) const override {
    T *t = (T *)peek_key_block_->read(pos + start_key_idx_offest_);
    return *t;
  };
  void seekToFirst() override {
    cur_idx_ = 0;
  }
  T key() override {
    T *t = (T *)cur_key_block_->read(cur_idx_ + start_key_idx_offest_);
    return *t;
  };
  uint64_t current_pos() const override { return cur_idx_; };
  std::string id() override { return id_; }
  uint64_t num_keys() const override { return num_keys_; }
  virtual uint64_t bulkReadAndForward(uint64_t keys_to_copy, char **data,
                                      uint64_t *len) {
    abort();
    return 0;
#if 0
    keys_to_copy = std::min(keys_to_copy, (uint64_t)MAX_KEYS_TO_BULK_COPY);
    keys_to_copy = std::min(keys_to_copy, num_keys_ - cur_idx_);
    ssize_t bytes_read =
        pread(file_descriptor_, bulk_key_buffer_, keys_to_copy * key_size_,
              cur_idx_ * key_size_ + start_offset_byte_);
    cur_idx_ += keys_to_copy;
    buf_idx_ = 0;
    cur_key_loaded_ = false;
    if (bytes_read == -1) {
      perror("pread");
      abort();
    }
    *len = bytes_read;
    *data = bulk_key_buffer_;
    return keys_to_copy;
#endif
  };

  Iterator<T> *subRange(uint64_t start_key_idx, uint64_t end_key_idx) override {
    return new IntDiskIterator(
        file_descriptor_, start_key_idx, end_key_idx - start_key_idx,
        id_ + "[" + std::to_string(start_key_idx) + "," + std::to_string(end_key_idx) + "]");
  }

 private:
  int file_descriptor_;
  uint64_t cur_idx_;
  uint64_t num_keys_;
  uint64_t start_key_idx_offest_;
  FileKeyBlock *cur_key_block_;
  FileKeyBlock *peek_key_block_;
  // char *bulk_key_buffer_;
  std::string id_;
};

template <class T>
class IntDiskBuilder : public IteratorBuilder<T> {
 public:
  IntDiskBuilder(const char *file_name, size_t buffer_size, std::string id)
      : file_name_(new char[strlen(file_name) + 1]),
        buffer_size_(buffer_size),
        key_size_(sizeof(T)),
        num_keys_(0),
        buffer_idx_(0),
        id_(id),
        file_offset_(0),
        start_offset_byte_(0) {
    memcpy(file_name_, file_name, strlen(file_name));
    file_name_[strlen(file_name)] = '\0';
    file_descriptor_ = open(file_name_, O_WRONLY | O_CREAT, 0644);
    if (file_descriptor_ == -1) {
      perror("popen");
      abort();
    }
    buffer_ = new char[buffer_size_];
  }

  IntDiskBuilder(const char *file_name, size_t buffer_size,
                 uint64_t start_offset_byte, std::string id)
      : file_name_(new char[strlen(file_name) + 1]),
        buffer_size_(buffer_size),
        key_size_(sizeof(T)),
        num_keys_(0),
        buffer_idx_(0),
        id_(id),
        file_offset_(0),
        start_offset_byte_(start_offset_byte) {
    memcpy(file_name_, file_name, strlen(file_name));
    file_name_[strlen(file_name)] = '\0';
    file_descriptor_ = open(file_name_, O_WRONLY | O_CREAT, 0644);
    if (file_descriptor_ == -1) {
      perror("popen");
      abort();
    }
    buffer_ = new char[buffer_size_];
  }

  void add(const T &t) override {
    num_keys_++;
    if (key_size_ + buffer_idx_ < buffer_size_) {
      addKeyToBuffer(t);
      return;
    }
    flushBufferToDisk();
    addKeyToBuffer(t);
  }
  Iterator<T> *build() override {
    flushBufferToDisk();
    int read_only_fd = open(file_name_, O_RDONLY);
    if (read_only_fd == -1) {
      perror("popen");
      abort();
    }
    return new IntDiskIterator<T>(read_only_fd, num_keys_, id_);
  }

  void bulkAdd(Iterator<T> *iter, uint64_t keys_to_add) override {
    flushBufferToDisk();
    char *data;
    uint64_t len;
    while (keys_to_add > 0) {
      uint64_t keys_added = iter->bulkReadAndForward(keys_to_add, &data, &len);
      keys_to_add -= keys_added;
      num_keys_ += keys_added;
      ssize_t bytes_written = pwrite(file_descriptor_, data, len,
                                     file_offset_ + start_offset_byte_);
      if (bytes_written == -1) {
        perror("pwrite");
        abort();
      }
      file_offset_ += bytes_written;
    }
  }

  IteratorBuilder<T> *subRange(uint64_t start, uint64_t end) override {
    num_keys_ = num_keys_ + (end - start);
    return new IntDiskBuilder(
        file_name_, buffer_size_, start * key_size_,
        id_ + "[" + std::to_string(start) + "," + std::to_string(end) + "]");
  }

 private:
  char *file_name_;
  int file_descriptor_;
  char *buffer_;
  int key_size_;
  size_t buffer_idx_;
  size_t buffer_size_;
  off_t file_offset_;
  uint64_t start_offset_byte_;
  uint64_t num_keys_;
  std::string id_;

  void addKeyToBuffer(const T &key) {
    memcpy(buffer_ + buffer_idx_, (char *)&key, key_size_);
    buffer_idx_ += key_size_;
  }

  void flushBufferToDisk() {
    if (buffer_idx_ == 0) return;
    ssize_t bytes_written = pwrite(file_descriptor_, buffer_, buffer_idx_,
                                   file_offset_ + start_offset_byte_);
    if (bytes_written == -1) {
      perror("pwrite");
      abort();
    }
    file_offset_ += buffer_idx_;
    buffer_idx_ = 0;
  }
};

#endif

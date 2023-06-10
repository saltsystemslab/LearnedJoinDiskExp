#ifndef INT_DISK_H
#define INT_DISK_H

#include "iterator.h"

template <class T>
class IntDiskIterator : public Iterator<T> {
 public:
  IntDiskIterator(int file_descriptor, uint64_t num_keys, std::string id)
      : file_descriptor_(file_descriptor),
        cur_key_buffer_(new char[sizeof(T)]),
        peek_key_buffer_(new char[sizeof(T)]),
        cur_key_loaded_(false),
        id_(id),
        key_size_(sizeof(T)),
        num_keys_(num_keys) {}
  ~IntDiskIterator() {}
  bool valid() const override { return cur_idx_ < num_keys_; }
  void next() override {
    cur_idx_++;
    cur_key_loaded_ = false;
  }
  T peek(uint64_t pos) const override {
    ssize_t bytes_read = pread(file_descriptor_, peek_key_buffer_, key_size_,
                               cur_idx_ * key_size_);
    if (bytes_read == -1) {
      perror("pread");
      abort();
    }
    return *(T *)(peek_key_buffer_);
  };
  void seekToFirst() override { cur_idx_ = 0; };
  T key() override {
    if (!cur_key_loaded_) {
      ssize_t bytes_read = pread(file_descriptor_, cur_key_buffer_, key_size_,
                                 cur_idx_ * key_size_);
      if (bytes_read == -1) {
        perror("pread");
        abort();
      }
      cur_key_loaded_ = true;
    }
    return *(T *)(cur_key_buffer_);
  };
  uint64_t current_pos() const override { return cur_idx_; };
  std::string id() override { return id_; }
  uint64_t num_keys() const override { return num_keys_; }

 private:
  int file_descriptor_;
  int key_size_;
  uint64_t cur_idx_;
  uint64_t num_keys_;
  char *cur_key_buffer_;
  char *peek_key_buffer_;
  bool cur_key_loaded_;
  std::string id_;
};

#if 0
template <class T>
class BulkReadIntDiskIterator : public BulkReadIterator<T> {
 public:
  BulkReadIntDiskIterator(T *a, uint64_t num_keys, std::string id)
      : id_(id), num_keys_(num_keys), cur_(0), a_(a) {}
  virtual bool valid() const override { return cur_ < num_keys_; }
  virtual uint64_t current_pos() const override { return cur_; }
  virtual uint64_t num_keys() const override { return num_keys_; }
  virtual T key() override { return a_[cur_]; }
  virtual uint64_t bulkReadAndForward(uint64_t num_keys, char **data,
                                      uint64_t *len) {
    *data = (char *)(a_ + cur_);
    *len = num_keys_ * sizeof(T);
    cur_ += num_keys_;
    return num_keys_;
  };
  virtual std::string id() { return id_; }

 private:
  T *a_;
  uint64_t num_keys_;
  uint64_t cur_;
  std::string id_;
};
#endif

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
  };
  BulkReadIterator<T> *buildBulkIterator() override {
    abort();
    return nullptr;
  };

 private:
  char *file_name_;
  int file_descriptor_;
  char *buffer_;
  int key_size_;
  size_t buffer_idx_;
  size_t buffer_size_;
  off_t file_offset_;
  uint64_t num_keys_;
  std::string id_;

  void addKeyToBuffer(const T &key) {
    memcpy(buffer_ + buffer_idx_, (char *)&key, key_size_);
    buffer_idx_ += key_size_;
  }
  void flushBufferToDisk() {
    ssize_t bytes_written =
        pwrite(file_descriptor_, buffer_, buffer_idx_, file_offset_);
    if (bytes_written == -1) {
      perror("pwrite");
      abort();
    }
    file_offset_ += buffer_idx_;
    buffer_idx_ = 0;
  }
};

#if 0
template <class T>
class BulkAddIntDiskIteratorBuilder : public BulkAddIteratorBuilder<T> {
 public:
  BulkAddIntDiskIteratorBuilder(uint64_t num_keys_, std::string id)
      : a_(new T[num_keys_]), id_(id) {}
  void bulkAdd(BulkReadIterator<T> *iter, uint64_t keys_to_add) override {
    T *buffer;
    uint64_t len;
    while (keys_to_add > 0) {
      uint64_t keys_added =
          iter->bulkReadAndForward(keys_to_add, &buffer, &len);
      keys_to_add -= keys_added;
      for (uint64_t i = 0; i < keys_added; i++) {
        a_[cur_++] = buffer[i];
      }
    }
  }
  Iterator<T> *finish() override {
    return new IntDiskBuilder<T>(a_, num_keys_, id_);
  }

 private:
  T *a_;
  uint64_t num_keys_;
  uint64_t cur_;
  std::string id_;
  bool finished_;
};
#endif

#endif

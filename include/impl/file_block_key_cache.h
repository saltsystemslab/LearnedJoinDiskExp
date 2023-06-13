#ifndef FILE_KEY_BLOCK_H
#define FILE_KEY_BLOCK_H


class FileKeyBlock {
  public:
  FileKeyBlock(int file_descriptor, uint32_t block_size, uint32_t key_size)
      : fd_(file_descriptor),
        block_size_(block_size),
        key_size_(key_size),
        keys_per_block_(block_size / key_size),
        buffer_(new char[block_size]),
        buffer_loaded_(false) {
    if (keys_per_block_ * key_size != block_size) {
      abort();
    }
  }

  char *read(uint64_t key_idx) {
    if (!buffer_loaded_ || key_idx < cur_block_start_idx_ ||
        key_idx > cur_block_end_idx_) {
      loadBuffer(key_idx);
    }
    return buffer_ + (key_idx - cur_block_start_idx_) * key_size_;
  }

 private:
  int fd_;
  char *buffer_;
  bool buffer_loaded_;
  uint32_t block_size_;
  uint32_t keys_per_block_;
  uint32_t key_size_;
  uint32_t cur_block_start_idx_;
  uint32_t cur_block_end_idx_;

  void loadBuffer(uint64_t key_idx) {
    uint64_t block_id = (key_idx * key_size_) / block_size_;
    pread(fd_, buffer_, block_size_, block_id * block_size_);
    cur_block_start_idx_ = block_id * keys_per_block_;
    cur_block_end_idx_ = cur_block_start_idx_ + keys_per_block_ - 1;
    buffer_loaded_ = true;
  }
};

#endif

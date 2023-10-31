#ifndef LEARNEDINDEXMERGE_FILE_PAGE_CACHE_H
#define LEARNEDINDEXMERGE_FILE_PAGE_CACHE_H

#define PAGE_SIZE 4096

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

class FilePageBuffer {
public:
  // Return a pointer to buffer containing bytes from that pge.
  // The buffer is valid atleast until the next get_page. Not Thread Safe.
  virtual char *get_page(uint64_t page_idx) = 0;
  virtual char *getLoadedPages(uint64_t *len) = 0;
  virtual uint64_t get_num_disk_fetches() = 0;
};


class FileNPageBuffer : public FilePageBuffer{
public:
  FileNPageBuffer(int fd, uint64_t start_offset, int buffer_size, bool always_load_ahead)
      : fd_(fd), 
      start_offset_(start_offset), 
      buffer_size_(buffer_size),
      is_buffer_loaded_(false),
      buffer_(new char[buffer_size * PAGE_SIZE]), 
      num_disk_fetches_(0),
      always_load_ahead_(always_load_ahead) {
        if (buffer_size_ == 0) {
          abort();
        }
      }
  ~FileNPageBuffer() { delete[] buffer_; }
  char *get_page(uint64_t page_idx) override {
    if (is_buffer_loaded_) {
      if (page_idx == cur_page_idx_) {
        return buffer_;
      }
      if (!always_load_ahead_ && cur_page_idx_ + buffer_size_ > page_idx) {
        return buffer_ + (page_idx - cur_page_idx_) * PAGE_SIZE;
      }
    }

    // We assume that a page can always be read in a single pread call.
    // If we go beyond, we fill the bits 1s. This won't work for all
    // comparators.
    int bytes_read;
    bytes_read = pread(fd_, buffer_, buffer_size_ * PAGE_SIZE,
                           start_offset_ + page_idx * PAGE_SIZE);
    if (bytes_read < buffer_size_ * PAGE_SIZE) {
      int bytes_to_fill = (buffer_size_ * PAGE_SIZE) - bytes_read;
      memset(buffer_ + bytes_read, -1, bytes_to_fill);
    }
    num_disk_fetches_++;
    if (bytes_read < 0) {
      perror("pread");
      abort();
    }
    cur_page_idx_ = page_idx;
    is_buffer_loaded_ = true;
    return buffer_;
  }

  uint64_t get_num_disk_fetches() override { return num_disk_fetches_; }

  char *getLoadedPages(uint64_t *len) override {
    *len = buffer_size_ * PAGE_SIZE;
    return buffer_;
  }

private:
  int buffer_size_;
  bool always_load_ahead_;
  int fd_;
  uint64_t start_offset_;
  bool is_buffer_loaded_;
  char *buffer_;
  uint64_t cur_page_idx_;
  uint64_t num_disk_fetches_;
};

#endif
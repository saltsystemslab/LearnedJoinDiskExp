#ifndef LEARNEDINDEXMERGE_FILE_PAGE_CACHE_H
#define LEARNEDINDEXMERGE_FILE_PAGE_CACHE_H

#define PAGE_SIZE 4096

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

class FilePageCache {
public:
  // Return a pointer to buffer containing bytes from that pge.
  // The buffer is valid atleast until the next get_page. Not Thread Safe.
  virtual char *get_page(uint64_t page_idx) = 0;
  virtual char *get_cur_page(uint64_t *len) = 0;
  virtual uint64_t get_num_disk_fetches() = 0;
};

class FileSinglePageCache : public FilePageCache {
public:
  FileSinglePageCache(int fd, uint64_t start_offset)
      : fd_(fd), start_offset_(start_offset), is_buffer_loaded_(false),
        buffer_(new char[PAGE_SIZE]), num_disk_fetches_(0) {}
  ~FileSinglePageCache() { delete[] buffer_; }
  char *get_page(uint64_t page_idx) override {
    if (!is_buffer_loaded_ || page_idx != cur_page_idx_) {
      // We assume that a page can always be read in a single pread call.
      int bytes_read =
          pread(fd_, buffer_, PAGE_SIZE, start_offset_ + (page_idx) * PAGE_SIZE);
      num_disk_fetches_++;
      if (bytes_read < 0) {
        perror("pread");
        abort();
      }
      cur_page_idx_ = page_idx;
      is_buffer_loaded_ = true;
    }
    return buffer_;
  }

  uint64_t get_num_disk_fetches() override {
    return num_disk_fetches_;
  }

  char *get_cur_page(uint64_t *len) {
    *len = PAGE_SIZE;
    return buffer_;
  }

private:
  int fd_;
  uint64_t start_offset_;
  bool is_buffer_loaded_;
  char *buffer_;
  uint64_t cur_page_idx_;
  uint64_t num_disk_fetches_;
};

class FileThreePageCache : public FilePageCache {
public:
  FileThreePageCache(int fd, uint64_t start_offset)
      : fd_(fd), start_offset_(start_offset), is_buffer_loaded_(false),
        buffer_(new char[3 * PAGE_SIZE]), num_disk_fetches_(0) {}
  ~FileThreePageCache() { delete[] buffer_; }
  char *get_page(uint64_t page_idx) override {
    if (!is_buffer_loaded_ || page_idx != cur_page_idx_) {
      // We assume that a page can always be read in a single pread call.
      // If we go beyond, we fill the bits 1s. This won't work for all comparators.
      int bytes_read;
      if (page_idx) {
          bytes_read = pread(fd_, buffer_, 3 * PAGE_SIZE, start_offset_ + (page_idx - 1) * PAGE_SIZE);
          if (bytes_read < 3 *PAGE_SIZE) {
            int bytes_to_fill = (3*PAGE_SIZE) - bytes_read;
            memset(buffer_ + bytes_read, -1, bytes_to_fill);
          }
      } else {
          memset(buffer_, 0, 3 * PAGE_SIZE);
          bytes_read = pread(fd_, buffer_ + PAGE_SIZE, 2 * PAGE_SIZE, start_offset_);
      }
      num_disk_fetches_++;
      if (bytes_read < 0) {
        perror("pread");
        abort();
      }
      cur_page_idx_ = page_idx;
      is_buffer_loaded_ = true;
    }
    return buffer_;
  }

  uint64_t get_num_disk_fetches() override {
    return num_disk_fetches_;
  }

  char *get_cur_page(uint64_t *len) {
    *len = 3 * PAGE_SIZE;
    return buffer_;
  }

private:
  int fd_;
  uint64_t start_offset_;
  bool is_buffer_loaded_;
  char *buffer_;
  uint64_t cur_page_idx_;
  uint64_t num_disk_fetches_;
};


#endif
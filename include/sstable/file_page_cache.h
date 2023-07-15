#ifndef LEARNEDINDEXMERGE_FILE_PAGE_CACHE_H
#define LEARNEDINDEXMERGE_FILE_PAGE_CACHE_H

#define PAGE_SIZE 4096

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

class FilePageCache {
public:
  // Return a pointer to buffer containing bytes from that pge.
  // The buffer is valid atleast until the next get_page. Not Thread Safe.
  virtual char *get_page(uint64_t page_idx) = 0;
};

class FileSinglePageCache : public FilePageCache {
public:
  FileSinglePageCache(int fd, uint64_t start_offset)
      : fd_(fd), start_offset_(start_offset), is_buffer_loaded_(false),
        buffer_(new char[PAGE_SIZE]) {}
  ~FileSinglePageCache() { delete[] buffer_; }
  char *get_page(uint64_t page_idx) override {
    if (!is_buffer_loaded_ || page_idx != cur_page_idx_) {
      // We assume that a page can always be read in a single pread call.
      int bytes_read =
          pread(fd_, buffer_, PAGE_SIZE, start_offset_ + page_idx * PAGE_SIZE);
      if (bytes_read < 0) {
        perror("pread");
        abort();
      }
      cur_page_idx_ = page_idx;
      is_buffer_loaded_ = true;
    }
    return buffer_;
  }

private:
  int fd_;
  uint64_t start_offset_;
  bool is_buffer_loaded_;
  char *buffer_;
  uint64_t cur_page_idx_;
};

#endif
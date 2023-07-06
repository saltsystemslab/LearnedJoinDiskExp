#ifndef SSTABLE_H
#define SSTABLE_H

#include "sort.h"
#include "uniform_random.h"
#include <filesystem>

using namespace std;

void read_sstable(std::string sstable, char *rand_nums,
                  uint64_t key_len_bytes) {
  int fd = open(sstable.c_str(), O_RDONLY);
  if (fd == -1) {
    perror("popen");
    abort();
  }
  uint64_t bytes_read = 0;
  while (bytes_read < key_len_bytes) {
    bytes_read += pread(fd, rand_nums + bytes_read, key_len_bytes - bytes_read,
                        bytes_read);
    printf("Read: %ld\n", bytes_read);
  }
  close(fd);
}

std::string get_sstable_path(std::string dir, std::string prefix,
                             uint64_t num_keys, uint64_t key_size) {
  return dir + "/" + prefix + "_" + std::to_string(num_keys) + "_" +
         std::to_string(key_size);
}

void write_sstable(std::string sstable_path, char *buf, uint64_t len) {
  uint64_t bytes_written = 0;
  int fd = open(sstable_path.c_str(), O_WRONLY | O_CREAT, 0644);
  if (fd == -1) {
    perror("popen");
    abort();
  }
  while (bytes_written < len) {
    bytes_written +=
        pwrite(fd, buf + bytes_written, len - bytes_written, bytes_written);
  }
  close(fd);
}

int get_sstable_read_only_fd(std::string sstable_path) {
  int read_only_fd = open(sstable_path.c_str(), O_RDONLY);
  if (read_only_fd == -1) {
    perror("popen");
    abort();
  }
  return read_only_fd;
}

#endif
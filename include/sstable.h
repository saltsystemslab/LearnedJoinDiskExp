#ifndef SSTABLE_H
#define SSTABLE_H

#include <string>
#include <filesystem>
#include "sort.h"
#include "uniform_random.h"

using namespace std;

void read_sstable(std::string sstable, char *rand_nums,
                  uint64_t key_len_bytes) {
  int fd = open(sstable.c_str(), O_RDONLY);
  uint64_t bytes_read = 0;
  while (bytes_read < key_len_bytes) {
	 bytes_read += pread(fd, rand_nums + bytes_read, key_len_bytes - bytes_read, bytes_read);
	 printf("Read: %ld\n", bytes_read);
  }
  close(fd);
}

std::string get_sstable_path(std::string dir, std::string prefix,
                             uint64_t num_keys, uint64_t key_size) {
  return dir + "/" + prefix + "_" + std::to_string(num_keys) + "_" +
         std::to_string(key_size);
}

char *read_or_create_sstable_into_mem(std::string dir, std::string sstable_name, uint64_t num_keys,
                    int key_len_bytes, int num_sort_threads) {
  if (num_keys == 0) {
    return nullptr;
  }
  uint64_t bytes_to_alloc = num_keys * key_len_bytes;
  char *rand_nums = new char[bytes_to_alloc];

  std::string sstable_path = get_sstable_path(dir, sstable_name, num_keys, key_len_bytes);
  bool is_sorted = false;
  printf("Generating rand bytes!\n");
  if (std::filesystem::exists(sstable_path)) {
    read_sstable(sstable_path, rand_nums, num_keys * key_len_bytes);
    is_sorted = true;
  } else {
    fill_rand_bytes(rand_nums, bytes_to_alloc);
    is_sorted = false;
  }
  printf("Done Generating rand bytes!\n");
  /*
  for (uint64_t i = 0; i < bytes_to_alloc; i += key_len_bytes) {
    std::string key(rand_nums + i, key_len_bytes);
    keys.push_back(key);
  }
  */
  if (!is_sorted) {
    printf("Beginning sorting\n");
    p_qsort(num_sort_threads, rand_nums, num_keys, key_len_bytes);
    printf("Finished sorting\n");
    int fd = open(sstable_path.c_str(), O_WRONLY | O_CREAT, 0644);
    printf("Beginning write\n");
    uint64_t bytes_written = 0;
    while (bytes_written < bytes_to_alloc) {
	    bytes_written += pwrite(fd, rand_nums + bytes_written, bytes_to_alloc, bytes_written);
	    printf("Written: %lu\n", bytes_written);
    }
    printf("Finished write\n");
    close(fd);
  }
  return rand_nums;
}

#endif
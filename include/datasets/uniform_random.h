#ifndef UNIFORM_RANDOM_H
#define UNIFORM_RANDOM_H

#include <filesystem>
#include <queue>
#include <stack>
#include <thread>
#include <vector>

#include "config.h"
#include "iterator.h"
#include "slice.h"
#include "sstable.h"
#include "slice_comparator.h"
#include "test_input.h"
#include "model_train.h"

using namespace std;

// Fills the iterators in BenchmarkInput with uniform random data.
void fill_uniform_input_lists(BenchmarkInput *input);

void fill_rand_bytes(char *v, uint64_t n) {
  std::random_device rd;  // a seed source for the random number engine
  std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
  uint64_t u = -1;        // All 1's
  std::uniform_int_distribution<uint64_t> distrib(0, u);
  for (uint64_t i = 0; i < n; i += 8) {
    uint64_t key = distrib(gen);
    memcpy(v + i, &key, 8);
  }
}

// If SSTable exists, read from it.
// Else, generate uniform random data and create the SSTable.
char *load_or_create_uniform_sstable(std::string dir, std::string sstable_name,
                                     uint64_t num_keys, int key_len_bytes, Comparator<Slice> *c) {
  if (num_keys == 0) {
    return nullptr;
  }
  uint64_t bytes_to_alloc = num_keys * key_len_bytes;
  char *rand_nums = new char[bytes_to_alloc];
  std::string sstable_path =
      get_sstable_path(dir, sstable_name, num_keys, key_len_bytes);
  if (std::filesystem::exists(sstable_path)) {
    read_sstable(sstable_path, rand_nums, num_keys * key_len_bytes);
    return rand_nums;
  }
  fill_rand_bytes(rand_nums, bytes_to_alloc);
  p_qsort(rand_nums, num_keys, key_len_bytes, c);
  write_sstable(sstable_path, rand_nums, num_keys * key_len_bytes);
  return rand_nums;
}

void fill_uniform_input_lists(BenchmarkInput *test_input) {
  std::string test_dir = test_input->test_dir;
  std::string result_file = test_input->result_file;
  uint64_t num_common_keys = test_input->num_common_keys;
  int key_size_bytes = test_input->key_size_bytes;
  SliceToPlrPointConverter *converter = test_input->slice_to_point_converter;
  int num_of_lists = test_input->num_of_lists;
  vector<uint64_t> list_sizes = test_input->list_sizes;
  Comparator<Slice> *comparator = test_input->comparator;
  int plr_error_bound = test_input->plr_error_bound;
  int is_disk_backed = test_input->is_disk_backed;
  test_input->total_input_keys_cnt = 0;

  char *common_keys = load_or_create_uniform_sstable(
      test_dir, "common", num_common_keys, key_size_bytes, comparator);
  for (int i = 0; i < num_of_lists; i++) {
    std::string sstable_path = get_sstable_path(
      test_dir, "sstable_" + std::to_string(i), list_sizes[i], key_size_bytes);
    // Key Creation is costly as files are large. So we reuse files as much as
    // possble. We expect the testbench to first create all sstable files in a
    // single run using 'no_op' merge. Then reuse the sstable files as is.
    // TODO: Move test_case input generation as it's own binary.
    char *keys = load_or_create_uniform_sstable(
        test_dir, "sstable_" + std::to_string(i), list_sizes[i], key_size_bytes, comparator);
    test_input->total_input_keys_cnt += list_sizes[i];
    if (num_common_keys) {
      keys = merge(keys, list_sizes[i], common_keys, num_common_keys,
                   key_size_bytes, comparator);
      test_input->total_input_keys_cnt += num_common_keys;
      sstable_path += "_with_" + std::to_string(num_common_keys) + "_common";
      write_sstable(sstable_path, keys,
                    (list_sizes[i] + num_common_keys) * key_size_bytes);
    }

    Iterator<Slice> *it;
    if (is_disk_backed) {
      it = new SliceFileIterator(get_sstable_read_only_fd(sstable_path), 0,
                                list_sizes[i], key_size_bytes, sstable_path);
    } else {
      it = new SliceArrayIterator(keys, list_sizes[i] + num_common_keys,
                                 key_size_bytes, "inmem:" + sstable_path);
    }

    Model<Slice> *m = train_model(it, test_input);
    test_input->iterators_with_model[i] = new IteratorWithModel(it, m);
    test_input->iterators[i] = it;
  }

  if (is_disk_backed) {
    std::string result_sstable = test_dir + "/" + result_file;
    test_input->resultBuilder = new SliceFileIteratorBuilder(result_sstable.c_str(), 4096, key_size_bytes, "result");
  } else {
    test_input->resultBuilder = new SliceArrayBuilder(test_input->total_input_keys_cnt, key_size_bytes, "result");
  }
}

#endif

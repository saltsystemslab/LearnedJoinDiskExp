#ifndef REAL_WORLD_H
#define REAL_WORLD_H

#include "sort.h"
#include "slice_file_iterator.h"
#include "iterator_with_model.h"
#include "model_train.h"
#include <set>

#define HEADER_SIZE 16

// Creates iterators for BenchmarkInput using a datafile
// Splits keys from this datafile across iterators by randomly selecting
// a fraction of keys specified in BenchmarkInput.
void fill_lists_from_datafile(BenchmarkInput *input);

void read_sstable_with_header(char **v, uint64_t *num_keys, uint16_t *key_size_bytes, std::string datafile) {
  int fd = open(datafile.c_str(), O_RDONLY);
  if (fd == -1) {
    perror("popen");
    abort();
  }

  // First 8 bytes for number of keys.
  pread(fd, (char *)num_keys, 8, 0);
  // .. Next 2 bytes for key_len.
  pread(fd, (char *)key_size_bytes, 2, 8);

  uint64_t bytes_to_alloc = (*num_keys) * (*key_size_bytes);
  char *buf = new char[bytes_to_alloc];
  uint64_t bytes_read = 0;
  while (bytes_read < bytes_to_alloc) {
    bytes_read += pread(fd, buf + bytes_read, bytes_to_alloc - bytes_read,
        bytes_read + HEADER_SIZE);
  }
  *v = buf;
}

void read_sstable_header(uint64_t *num_keys, uint16_t *key_size_bytes, std::string datafile) {
  int fd = open(datafile.c_str(), O_RDONLY);
  if (fd == -1) {
    perror("popen");
    abort();
  }

  // First 8 bytes for number of keys.
  pread(fd, (char *)num_keys, 8, 0);
  // .. Next 2 bytes for key_len.
  pread(fd, (char *)key_size_bytes, 2, 8);
  close(fd);
}

Iterator<Slice> *split_keys_from_datafile(
    IteratorBuilder<Slice> *ib,
    Iterator<Slice> *data_to_split,
    uint64_t num_keys, 
    int key_size_bytes, 
    set<uint64_t> split_keys,
    bool include_split_keys
  ) {

  if (include_split_keys) {
    for (auto k: split_keys) {
      ib->add(data_to_split->peek(k));
    }
  }
  else {
    uint64_t idx = 0;
    auto split_keys_iter = split_keys.begin();
    data_to_split->seekToFirst();
    while (data_to_split->valid()) {
      if (split_keys_iter == split_keys.end() || idx != *split_keys_iter) {
        ib->add(data_to_split->key());
      } else {
        while (split_keys_iter != split_keys.end() && idx == *split_keys_iter) {
          split_keys_iter++;
        }
      }       
      idx++;
      data_to_split->next();
    }
  } 
  return ib->build();
}

std::set<uint64_t> generate_uniform_split(uint64_t num_keys, double split) {
  std::random_device rd;  // a seed source for the random number engine
  std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
  std::uniform_int_distribution<uint64_t> distrib(0, num_keys);

  uint64_t num_split_keys = split * num_keys;
  set<uint64_t> split_keys;
  for (uint64_t i = 0; i < num_split_keys; i++) {
    uint64_t key = distrib(gen);
    split_keys.insert(key);
  }
  return split_keys;
}

void fill_lists_from_datafile(BenchmarkInput *input) {
    uint64_t num_keys;
    uint16_t key_size_bytes;
    read_sstable_header(&num_keys, &key_size_bytes, input->datafile_path);

    // Indexes of keys that will go into first list.
    // The second list might include them (for a join) or exclude these keys.
    set<uint64_t> split_keys_index = generate_uniform_split(num_keys, input->split_fraction);

    int fd = open(input->datafile_path.c_str(), O_RDONLY);
    SliceFileIterator datafile(fd, HEADER_SIZE, num_keys, key_size_bytes, "dataset");

    std::string iterator_0_name = "iterator_0";
    IteratorBuilder<Slice> *ib1;
    if (input->is_disk_backed) {
      std::string iterator_file = input->test_dir + "/" + iterator_0_name;
      ib1 = new SliceFileIteratorBuilder(iterator_file.c_str(), 4096, key_size_bytes, iterator_0_name);
    } else {
      ib1 = new SliceArrayBuilder(split_keys_index.size(), key_size_bytes, iterator_0_name);
    }
    Iterator<Slice> *it1 = split_keys_from_datafile(
      ib1, &datafile, num_keys, key_size_bytes, split_keys_index, true);
    Model<Slice> *m1 = train_model(it1, input);
    input->iterators_with_model[0] = new IteratorWithModel(it1, m1);
    input->iterators[0] = it1;

    std::string iterator_1_name = "iterator_1";
    IteratorBuilder<Slice> *ib2;
    if (input->is_disk_backed) {
      std::string iterator_file = input->test_dir + "/" + iterator_1_name;
      ib2 = new SliceFileIteratorBuilder(iterator_file.c_str(), 4096, key_size_bytes, iterator_1_name);
    } else {
      ib2 = new SliceArrayBuilder(split_keys_index.size(), key_size_bytes, iterator_1_name);
    }
    bool include_split_keys = input->is_join();
    Iterator<Slice> *it2; 
    if (input->is_join()) {
      it2 = split_keys_from_datafile(
        ib2, &datafile, num_keys, key_size_bytes, set<uint64_t>(), false);
    } else {
      it2 = split_keys_from_datafile(
        ib2, &datafile, num_keys, key_size_bytes, split_keys_index, false);
    }
    
    Model<Slice> *m2 = train_model(it2, input);
    input->iterators_with_model[1] = new IteratorWithModel(it2, m2);
    input->iterators[1] = it2;

    if (input->is_disk_backed) {
      std::string result_sstable = input->test_dir + "/" + input->result_file;
      input->resultBuilder = new SliceFileIteratorBuilder(result_sstable.c_str(), 4096, key_size_bytes, "result");
    } else {
      input->resultBuilder = new SliceArrayBuilder(num_keys, key_size_bytes, "result");
    }
}

#endif

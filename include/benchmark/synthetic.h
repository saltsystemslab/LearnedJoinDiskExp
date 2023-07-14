#ifndef LEARNEDINDEXMERGE_SYNTHETIC_H
#define LEARNEDINDEXMERGE_SYNTHETIC_H

#include <openssl/rand.h>

#include <algorithm>
#include <execution>

#include "comparator.h"
#include "key_value_slice.h"
#include "sstable.h"

namespace li_merge {

char *
create_uniform_random_distribution_buffer(uint64_t num_keys, int key_size_bytes,
                                          int value_size_bytes,
                                          Comparator<KVSlice> *comparator) {
  uint64_t buf_size = num_keys * (key_size_bytes + value_size_bytes);
  char *data = new char[buf_size];
  RAND_bytes((unsigned char *)data, buf_size);
  return data;
}

char **sort_buffer(char *data, uint64_t num_keys, int key_size_bytes,
                   int value_size_bytes, Comparator<KVSlice> *comparator) {
  char **data_ptrs = new char *[num_keys];
  for (uint64_t i = 0; i < num_keys; i++) {
    data_ptrs[i] = data + (i * (key_size_bytes + value_size_bytes));
  }
  std::sort(std::execution::par_unseq, data_ptrs, data_ptrs + num_keys,
            [key_size_bytes, comparator](const char *a, const char *b) -> bool {
              KVSlice kv1(a, key_size_bytes, 0);
              KVSlice kv2(b, key_size_bytes, 0);
              return comparator->compare(kv1, kv2) <= 0;
            });
  return data_ptrs;
}

SSTable<KVSlice> *generate_uniform_random_distribution(
    uint64_t num_keys, int key_size_bytes, int value_size_bytes,
    Comparator<KVSlice> *comparator, SSTableBuilder<KVSlice> *builder) {
  char *data = create_uniform_random_distribution_buffer(
      num_keys, key_size_bytes, value_size_bytes, comparator);
  char **data_ptrs =
      sort_buffer(data, num_keys, key_size_bytes, value_size_bytes, comparator);
  for (uint64_t i = 0; i < num_keys; i++) {
    KVSlice kv(data_ptrs[i], key_size_bytes, value_size_bytes);
    builder->add(kv);
  }
  delete[] data_ptrs;
  delete[] data;
  return builder->build();
}
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_SYNTHETIC_H

#include <gtest/gtest.h>

#include "disk_sstable.h"
#include "synthetic.h"

namespace li_merge {
TEST(DiskSSTable, TestCreation) {
  uint64_t num_elts = 5000;
  int key_size_bytes = 16;
  int value_size_bytes = 16;
  Comparator<KVSlice> *comparator = new KVSliceMemcmp();
  SSTable<KVSlice> *table = generate_uniform_random_distribution(
      num_elts, key_size_bytes, value_size_bytes, comparator,
      new FixedSizeKVDiskSSTableBuilder("test_sstable", key_size_bytes,
                                        value_size_bytes));

  Iterator<KVSlice> *iterator = table->iterator();
  iterator->seekToFirst();
  while (iterator->valid()) {
    iterator->next();
  }
  ASSERT_EQ(iterator->num_elts(), 5000);
}
}; // namespace li_merge

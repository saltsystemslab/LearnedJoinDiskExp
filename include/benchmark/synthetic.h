//
// Created by chesetti on 7/14/23.
//

#ifndef LEARNEDINDEXMERGE_SYNTHETIC_H
#define LEARNEDINDEXMERGE_SYNTHETIC_H

#include "../sstable/key_value_slice.h"

namespace li_merge {

void generate_uniform_random_distribution(uint64_t num_keys, int key_size_bytes,
                                          int value_size_bytes,
                                          Comparator<> *comparator,
                                          SSTableBuilder *builder);
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_SYNTHETIC_H

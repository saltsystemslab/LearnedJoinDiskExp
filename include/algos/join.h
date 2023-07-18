#ifndef LEARNEDINDEXMERGE_JOIN_H
#define LEARNEDINDEXMERGE_JOIN_H

#include "comparator.h"
#include "index.h"
#include "iterator.h"
#include "sstable.h"

namespace li_merge {
template <class T>
SSTable<T> *indexed_nested_loop_join(SSTable<T> *outer,
                             SSTable<T> *inner,
                             Index<T> *inner_index,
                             Comparator<T> *comparator,
                             SSTableBuilder<T> *result) {
  auto outer_iterator = outer->iterator();
  auto inner_iterator = inner->iterator();
  outer_iterator->seekToFirst();
  inner_iterator->seekToFirst();
  uint64_t inner_num_elts = inner_iterator->num_elts();
  while (outer_iterator->valid()) {
    uint64_t approx_pos = inner_index->getApproxPosition(inner_iterator->key());
    approx_pos = std::min(approx_pos, inner_num_elts-1);
    // Set to first value lesser than or equal to the inner key.
    bool is_overshoot = false;
    while (approx_pos && comparator->compare(inner_iterator->peek(approx_pos),
                                             outer_iterator->key()) > 0) {
      is_overshoot = true;
      approx_pos--;
    }
    // We could have undershot before, so go ahead till you meet key greater
    // than current key. If overshot, then there's no need to do this
    // correction.
    while (!is_overshoot && approx_pos < inner_iterator->num_elts() &&
           comparator->compare(inner_iterator->peek(approx_pos),
                               inner_iterator->key()) < 0) {
      approx_pos++;
    }

    // No more keys in larger list.
    if (approx_pos == inner_iterator->num_elts()) {
      break;
    }
    if (comparator->compare(inner_iterator->peek(approx_pos),
                            outer_iterator->key()) == 0) {
      result->add(inner_iterator->key());
    }
    outer_iterator->next();
  }
  return result->build();
}

template <class T>
SSTable<T> *presorted_merge_join(SSTable<T> *outer,
                         SSTable<T> *inner, Comparator<T> *comparator,
                         SSTableBuilder<T> *result_builder) {
  auto outer_iterator = outer->iterator();
  auto inner_iterator = inner->iterator();
  outer_iterator->seekToFirst();
  inner_iterator->seekToFirst();
  while (outer_iterator->valid()) {
    while (inner_iterator->valid() &&
           (comparator->compare(inner_iterator->key(), outer_iterator->key()) <
            0)) {
      inner_iterator->next();
    }
    if (!inner_iterator->valid()) {
      break;
    }
    if (comparator->compare(outer_iterator->key(), inner_iterator->key()) ==
        0) {
      result_builder->add(inner_iterator->key());
      inner_iterator->next();
    }
    outer_iterator->next();
  }
  return result_builder->build();
}

} // namespace li_merge

#endif
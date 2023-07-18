#ifndef LEARNEDINDEXMERGE_JOIN_H
#define LEARNEDINDEXMERGE_JOIN_H

#include "comparator.h"
#include "index.h"
#include "iterator.h"
#include "sstable.h"

namespace li_merge {
template <class T>
int indexed_nested_loop_join(Iterator<T> *outer_iterator,
                             Iterator<T> *inner_iterator,
                             Iterator<T> *inner_index,
                             Comparator<T> *comparator,
                             SSTableBuilder<T> *result) {
  outer_iterator->seekToFirst();
  inner_iterator->seekToFirst();
  while (outer_iterator->valid()) {
    uint64_t approx_pos = inner_index->guessPosition(inner_iterator->key());
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
    if (approx_pos == inner_iterator->num_keys()) {
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
int presorted_merge_join(Iterator<T> *outer_iterator,
                         Iterator<T> *inner_iterator, Comparator<T> *comparator,
                         SSTableBuilder<T> *result_builder) {
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
#ifndef SORT_MERGE_LEARNED_JOIN_H
#define SORT_MERGE_LEARNED_JOIN_H

#include "comparator.h"
#include "config.h"
#include "iterator.h"
#include "iterator_with_model.h"

template <class T> class SortedMergeLearnedJoin {
public:
  // Writes into result all keys present in both smaller and larger.
  // Assumes that 1) smaller and larger are both sorted and 2) Keys are unique.
  static Iterator<T> *merge(IteratorWithModel<T> *smaller,
                            IteratorWithModel<T> *larger,
                            Comparator<T> *comparator,
                            IteratorBuilder<T> *result) {
    smaller->seekToFirst();
    larger->seekToFirst();
    printf("Sorted Merge Learn Join\n");
    while (smaller->valid()) {
      uint64_t approx_pos = larger->guessPositionMonotone(smaller->key());

      // Set to first value lesser than or equal to the smaller key.
      bool is_overshoot = false;
      while (approx_pos && comparator->compare(larger->peek(approx_pos),
                                               smaller->key()) > 0) {
        is_overshoot = true;
        approx_pos--;
      }
      // We could have undershot before, so go ahead till you meet key greater
      // than current key. If overshot, then there's no need to do this
      // correction.
      while (!is_overshoot && approx_pos < larger->num_keys() &&
             comparator->compare(larger->peek(approx_pos), smaller->key()) <
                 0) {
        approx_pos++;
      }

      // No more keys in larger list.
      if (approx_pos == larger->num_keys()) {
        break;
      }

      if (comparator->compare(larger->peek(approx_pos), smaller->key()) == 0) {
        result->add(smaller->key());
      }
      smaller->next();
    }
    return result->build();
  }
};

#endif

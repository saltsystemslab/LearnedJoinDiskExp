#ifndef SORT_MERGE_JOIN_H
#define SORT_MERGE_JOIN_H

#include "comparator.h"
#include "config.h"
#include "iterator.h"

template <class T>
class SortedMergeJoin {
 public:
  // Writes into result all keys present in both smaller and larger.
  // Assumes that 1) smaller and larger are both sorted and 2) Keys are unique.
  static Iterator<T> *merge(Iterator<T> *smaller, Iterator<T> *larger,
                            Comparator<T> *comparator,
                            IteratorBuilder<T> *result) {
    printf("Sorted Merge Join\n");
    smaller->seekToFirst();
    larger->seekToFirst();
    while (smaller->valid()) {
      while (larger->valid() &&
             (comparator->compare(larger->key(), smaller->key()) < 0)) {
        larger->next();
      }
      if (!larger->valid()) {
        break;
      }
      if (comparator->compare(smaller->key(), larger->key()) == 0) {
        result->add(smaller->key());
        larger->next();
      }
      smaller->next();
    }
    return result->build();
  }
};
#endif

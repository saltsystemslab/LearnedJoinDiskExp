#ifndef SORT_MERGE_BINARY_LOOKUP_H
#define SORT_MERGE_BINARY_LOOKUP_H

#include "comparator.h"
#include "config.h"
#include "iterator.h"
#include "standard_lookup.h"

// TODO: Remove this class once the 'model' for binary search exists.
template <class T>
class SortedMergeBinaryLookupJoin {
 public:
  // Writes into result all keys present in both smaller and larger.
  // Assumes that 1) smaller and larger are both sorted and 2) Keys are unique.
  static Iterator<T> *merge(Iterator<T> *smaller, Iterator<T> *larger,
                            Comparator<T> *comparator,
                            IteratorBuilder<T> *result) {
    printf("Sorted Merge Join with Binary Lookup\n");
    smaller->seekToFirst();
    uint64_t lo = 0;
    uint64_t hi = larger->num_keys() - 1;
    while (smaller->valid()) {
      lo = StandardLookup::lower_bound_binary_search(lo, hi, smaller->key(),
                                                     larger, comparator);
      if (comparator->compare(smaller->key(), larger->peek(lo)) == 0) {
        result->add(smaller->key());
      }
      smaller->next();
    }
    return result->build();
  }
};

#endif

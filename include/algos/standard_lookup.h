#ifndef LOOKUP_H
#define LOOKUP_H

#include "comparator.h"
#include "iterator.h"

class StandardLookup {
 public:
  template <class T>
  static uint64_t lower_bound(Iterator<T> *iterator, Comparator<T> *comparator,
                              T target_key) {
    return lower_bound_binary_search(0, iterator->num_keys() - 1, target_key,
                                     iterator, comparator);
  }

  template <class T>
  static bool lookup(Iterator<T> *iterator, Comparator<T> *comparator,
                     T target_key) {
    uint64_t l_b = lower_bound<T>(iterator, comparator, target_key);
    if (l_b >= iterator->num_keys_()) return false;
    return comparator->compare(target_key, iterator->peek(l_b)) == 0;
  }

  template <class T>
  static uint64_t lower_bound_binary_search(uint64_t start, uint64_t end,
                                            T target_key, Iterator<T> *iterator,
                                            Comparator<T> *comparator) {
    while (start < end) {
      uint64_t mid = start + (end - start) / 2;
      if (comparator->compare(iterator->peek(mid), target_key) < 0) {
        start = mid + 1;
      } else {
        end = mid;
      }
    }
    // If all values were less than target_key in the range, return outside the
    // range.
    if (comparator->compare(iterator->peek(start), target_key) < 0) {
      return start + 1;
    }
    return start;
  }
};

#endif

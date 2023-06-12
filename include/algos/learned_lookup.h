#ifndef LEARNEDLOOKUP_H
#define LEARNEDLOOKUP_H

#include "comparator.h"
#include "iterator_with_model.h"

class LearnedLookup {
 public:
  template <class T>
  static uint64_t lower_bound(IteratorWithModel<T> *iterator,
                          Comparator<T> *comparator, T target_key) {
    uint64_t num_keys = iterator->num_keys();
    uint64_t approx_pos = std::ceil(iterator->guessPositionUsingBinarySearch(target_key));
    uint64_t start = approx_pos - PLR_ERROR_BOUND;
    uint64_t end = approx_pos + PLR_ERROR_BOUND;

    if (start < 0) {
      start = 0;
    }
    if (end < 0) {
      end = 0;
    }

    if (start >= num_keys) {
      start = num_keys-1;
    }
    if (end >= num_keys) {
      end = num_keys-1;
    }

    while (comparator->compare(target_key, iterator->peek(start)) == -1) {
      start = start - PLR_ERROR_BOUND;
      if (start < 0) {
        start = 0;
        break;
      }
    }

    while (comparator->compare(target_key, iterator->peek(end)) == 1) {
      end = end + PLR_ERROR_BOUND;
      if (end >= num_keys) {
        end = num_keys - 1;
        break;
      }
    }
    return lower_bound_binary_search(start, end, target_key, iterator, comparator);
  }
  template <class T>
  static bool lookup(Iterator<T> *iterator, uint64_t num_keys,
                     Comparator<T> *comparator, T target_key) {
    uint64_t l_b = lower_bound<T>(iterator, comparator, target_key);
    if (l_b >= iterator->num_keys_()) return false;
    return comparator->compare(target_key, iterator->peek(l_b)) == 0;
  }

 private:
  template <class T>
  static uint64_t lower_bound_binary_search(uint64_t start, uint64_t end, T target_key,
                            IteratorWithModel<T> *iterator, Comparator<T> *comparator) {
    while (start < end) {
      uint64_t mid = start + (end-start) / 2;
      if (comparator->compare(iterator->peek(mid), target_key) < 0) {
        start = mid + 1;
      } else {
        end = mid;
      }
    }
    // If all values were less than target_key in the range, return outside the range.
    if (comparator->compare(iterator->peek(start), target_key) < 0) {
      return start + 1;
    }
    return start;
  }
};

#endif

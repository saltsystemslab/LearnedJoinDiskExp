#ifndef LEARNED_MERGE_BULK_H
#define LEARNED_MERGE_BULK_H

#include <fcntl.h>
#include <unistd.h>

#include <cmath>
#include <iostream>

#include "comparator.h"
#include "iterator.h"
#include "iterator_with_model.h"

#if TRACK_STATS
static int cluster_count_fd;
static int cluster_file_offset;
static int plr_error_fd;
static int plr_error_offset;
#endif

template <class T> class LearnedMergerBulk {
public:
  static Iterator<T> *merge(IteratorWithModel<T> **iterators, int n,
                            Comparator<T> *comparator,
                            IteratorBuilder<T> *result) {
    printf("LearnedBulkMerge!\n");
    for (int i = 0; i < n; i++) {
      iterators[i]->seekToFirst();
    }
    IteratorWithModel<T> *smallest = nullptr;
    IteratorWithModel<T> *second_smallest = nullptr;
    findTwoSmallest(iterators, n, comparator, &smallest, &second_smallest);

    while (smallest != nullptr) {
      // There's only one list left, so let's fill up all the way.
      if (second_smallest == nullptr) {
        while (smallest->valid()) {
          result->add(smallest->key());
          smallest->next();
        }
        smallest = nullptr;
        continue;
      }
      addClusterToResult(smallest, second_smallest, comparator, result);
      smallest = second_smallest;
      second_smallest = nullptr;
      findSecondSmallest(iterators, n, comparator, smallest, &second_smallest);
    }
    return result->build();
  }

private:
  static void addClusterToResult(IteratorWithModel<T> *smallest,
                                 IteratorWithModel<T> *second_smallest,
                                 Comparator<T> *comparator,
                                 IteratorBuilder<T> *merge_result_builder) {
    // approx_pos is always a valid position in iterator.
    uint64_t approx_pos = smallest->guessPositionMonotone(second_smallest->key());
    approx_pos = std::max(approx_pos, smallest->current_pos());
    bool is_overshoot = false;
    if (comparator->compare(smallest->peek(approx_pos),
                            second_smallest->key()) > 0) {
      approx_pos--;
      is_overshoot = true;
    }
    // Correct the overshoot
    if (is_overshoot) {
      while (comparator->compare(smallest->peek(approx_pos),
                                 second_smallest->key()) > 0) {
        approx_pos--;
      }
    }
    merge_result_builder->bulkAdd(smallest->get_iterator(),
                                  approx_pos - smallest->current_pos() + 1);
    // If we overshot, we copied all the items we wanted for this cluster....
    if (is_overshoot) {
      return;
    }
    // ....else, there might still be items to be taken from this list.
    while (smallest->valid() &&
           comparator->compare(smallest->key(), second_smallest->key()) <= 0) {
      merge_result_builder->add(smallest->key());
      smallest->next();
    }
  }

  static void findTwoSmallest(IteratorWithModel<T> **iterators, int n,
                              Comparator<T> *comparator, IteratorWithModel<T> **smallest,
                              IteratorWithModel<T> **second_smallest) {
    for (int i = 0; i < n; i++) {
      if (!iterators[i]->valid()) {
        continue;
      }
      if (*smallest == nullptr) {
        *smallest = iterators[i];
        continue;
      }
      if (comparator->compare((*smallest)->key(), iterators[i]->key()) > 0) {
        *second_smallest = *smallest;
        *smallest = iterators[i];
        continue;
      }
      if (*second_smallest == nullptr) {
        *second_smallest = iterators[i];
        continue;
      }
      if (comparator->compare((*second_smallest)->key(), iterators[i]->key()) >
          0) {
        *second_smallest = iterators[i];
        continue;
      }
    }
  }

  static void findSecondSmallest(IteratorWithModel<T> **iterators, int n,
                                 Comparator<T> *comparator,
                                 const IteratorWithModel<T> *smallest,
                                 IteratorWithModel<T> **second_smallest) {
    for (int i = 0; i < n; i++) {
      if (!iterators[i]->valid()) {
        continue;
      }
      if (smallest == iterators[i]) {
        continue;
      }
      if (*second_smallest == nullptr) {
        *second_smallest = iterators[i];
        continue;
      }
      if (comparator->compare((*second_smallest)->key(), iterators[i]->key()) >
          0) {
        *second_smallest = iterators[i];
        continue;
      }
    }
  }
};
#endif

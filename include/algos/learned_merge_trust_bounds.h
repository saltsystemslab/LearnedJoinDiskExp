#ifndef LEARNED_MERGE_TRUST_H
#define LEARNED_MERGE_TRUST_H

#include <fcntl.h>
#include <unistd.h>

#include <cassert>
#include <cmath>

#include "comparator.h"
#include "iterator.h"

template <class T> class LearnedMergerTrustBounds {
public:
  static Iterator<T> *test() { return nullptr; };
  static Iterator<T> *merge(Iterator<T> **iterators, int n,
                            Comparator<T> *comparator,
                            IteratorBuilder<T> *result) {
#if TRACK_STATS
    comparator = new CountingComparator<T>(comparator);
    cluster_count_fd = open("./DB/cluster.txt", O_WRONLY | O_CREAT | O_TRUNC,
                            S_IRUSR | S_IWUSR);
    plr_error_fd = open("./DB/plr_error.txt", O_WRONLY | O_CREAT | O_TRUNC,
                        S_IRUSR | S_IWUSR);
    cluster_file_offset = 0;
    plr_error_offset = 0;
#endif
    Iterator<T> *smaller_list = iterators[0];
    Iterator<T> *larger_list = iterators[1];
    printf("LearnedMergeTrustBounds!\n");
    smaller_list->seekToFirst();
    larger_list->seekToFirst();

    Iterator<T> *smallest = nullptr;
    Iterator<T> *second_smallest = nullptr;

    while (smaller_list->valid() &&
           comparator->compare(smaller_list->key(), larger_list->key()) <= 0) {
      KEY_TYPE *k1 = (KEY_TYPE *)smaller_list->key().data_;
      KEY_TYPE *k2 = (KEY_TYPE *)larger_list->key().data_;
#if USE_BULK_COPY
      result->bulkAdd(smaller_list, 1);
#else
      result->add(smaller_list->key());
      smaller_list->next();
#endif
    }

    while (smaller_list->valid()) {
      uint64_t approx_pos =
         larger_list->guessPositionMonotone(smaller_list->key());
      // Blind copy till approx_pos.
#if USE_BULK_COPY
      result->bulkAdd(larger_list, approx_pos - larger_list->current_pos());
#else
      while (larger_list->valid() && larger_list->current_pos() < approx_pos) {
        result->add(larger_list->key());
        larger_list->next();
      }
#endif
      // Copy the rest of the cluster.
      while (larger_list->valid() &&
             comparator->compare(larger_list->key(), smaller_list->key()) <=
                 0) {
#if USE_BULK_COPY
        result->bulkAdd(larger_list, 1);
#else
        result->add(larger_list->key());
        larger_list->next();
#endif
      }
#if USE_BULK_COPY
      result->bulkAdd(smaller_list, 1);
#else
      result->add(smaller_list->key());
      smaller_list->next();
#endif
    }

    while (larger_list->valid()) {
#if USE_BULK_COPY
      result->bulkAdd(larger_list, 1);
#else
      result->add(larger_list->key());
      larger_list->next();
#endif
    }

#if TRACK_STATS
    CountingComparator<T> *count_comp =
        dynamic_cast<CountingComparator<T> *>(comparator);
    std::cout << "Comparison Count: " << count_comp->get_count() << std::endl;
    close(cluster_count_fd);
    close(plr_error_fd);
#endif
    return result->finish();
  }
};
#endif

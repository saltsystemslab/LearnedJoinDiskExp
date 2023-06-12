#ifndef MERGE_PARALLEL_H
#define MERGE_PARALLEL_H

#include "comparator.h"
#include "config.h"
#include "iterator.h"
#include "standard_merge.h"
#include "standard_lookup.h"

class ParallelStandardMerger {
public:
  template <class T>
  static Iterator<T> *merge(
    Iterator<T> *iter1, 
    Iterator<T> *iter2, 
    int num_threads,
    Comparator<T> *comparator, 
    IteratorBuilder<T> *result) {

    uint64_t num_items = iter1->num_keys();
    uint64_t block_size = num_items / num_threads;
    uint64_t spill = num_items % num_threads;

    uint64_t iter1_start = 0;
    uint64_t iter2_start = 0;
    uint64_t result_start = 0;

    printf("Parallelized Standard Merge!\n");
    for (int i = 0; i < num_threads; i++) {
      uint64_t iter1_end = iter1_start + block_size;
      if (spill) {
        iter1_end++;
        spill--;
      }

      Iterator<T> *iter1_subrange = iter1->subRange(iter1_start, iter1_end);
      uint64_t iter2_end = StandardLookup::lower_bound<T>(iter2, comparator, iter1->peek(iter1_end-1));
      Iterator<T> *iter2_subrange = iter2->subRange(iter2_start, iter2_end);

      uint64_t result_end = result_start + (iter2_end - iter2_start) + (iter1_end - iter1_start);
      IteratorBuilder<T> *result_subrange = result->subRange(result_start, result_end);
      Iterator<T> *iterators[2] = {iter1_subrange, iter2_subrange};
      StandardMerger<T>::merge(iterators, 2, comparator, result_subrange);

      iter1_start = iter1_end;
      iter2_start = iter2_end;
      result_start = result_end;
    }

    if (iter2_start != iter2->num_keys()) {
      Iterator<T> *iter1_subrange = iter1->subRange(iter1_start, iter1_start);
      Iterator<T> *iter2_subrange = iter2->subRange(iter2_start, iter2->num_keys());
      uint64_t result_end = (iter2->num_keys() - iter2_start);
      IteratorBuilder<T> *result_subrange = result->subRange(result_start, result_end);
      Iterator<T> *iterators[2] = {iter1_subrange, iter2_subrange};
      StandardMerger<T>::merge(iterators, 2, comparator, result_subrange);
    }
    return result->build();
  }
};
#endif

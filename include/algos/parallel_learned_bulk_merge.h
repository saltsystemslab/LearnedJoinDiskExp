#ifndef MERGE_PARALLEL_LEARNED_BULK_H
#define MERGE_PARALLEL_LEARNED_BULK_H

#include "comparator.h"
#include "config.h"
#include "iterator.h"
#include "learned_merge.h"

class ParallelLearnedMergerBulk {
public:
  template <class T>
  static Iterator<T> *merge(
    IteratorWithModel<T> *iter1, 
    IteratorWithModel<T> *iter2, 
    int num_threads,
    Comparator<T> *comparator, 
    IteratorBuilder<T> *result) {

    uint64_t num_items = iter1->num_keys();
    uint64_t block_size = num_items / num_threads;
    uint64_t spill = num_items % num_threads;

    uint64_t iter1_start = 0;
    uint64_t iter2_start = 0;
    uint64_t result_start = 0;

    printf("Parallelized Learned Merge!\n");
    for (int i = 0; i < num_threads; i++) {
      uint64_t iter1_end = iter1_start + block_size;
      if (spill) {
        iter1_end++;
        spill--;
      }

      IteratorWithModel<T> *iter1_subrange = iter1->subRange(iter1_start, iter1_end);
      uint64_t iter2_end = iter2->lower_bound(iter1->peek(iter1_end-1));
      IteratorWithModel<T> *iter2_subrange = iter2->subRange(iter2_start, iter2_end);

      uint64_t result_end = result_start + (iter2_end - iter2_start) + (iter1_end - iter1_start);
      IteratorBuilder<T> *result_subrange = result->subRange(result_start, result_end);
      IteratorWithModel<T> *iterators[2] = {iter1_subrange, iter2_subrange};
      LearnedMergerBulk<T>::merge(iterators, 2, comparator, result_subrange);

      iter1_start = iter1_end;
      iter2_start = iter2_end;
      result_start = result_end;
    }

    if (iter2_start != iter2->num_keys()) {
      IteratorWithModel<T> *iter1_subrange = iter1->subRange(iter1_start, iter1_start);
      IteratorWithModel<T> *iter2_subrange = iter2->subRange(iter2_start, iter2->num_keys());
      uint64_t result_end = (iter2->num_keys() - iter2_start);
      IteratorBuilder<T> *result_subrange = result->subRange(result_start, result_end);
      IteratorWithModel<T> *iterators[2] = {iter1_subrange, iter2_subrange};
      LearnedMerger<T>::merge(iterators, 2, comparator, result_subrange);
    }
    return result->build();
  }
};
#endif

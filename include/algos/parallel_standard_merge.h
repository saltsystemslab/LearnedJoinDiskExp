#ifndef MERGE_PARALLEL_H
#define MERGE_PARALLEL_H

#include <unistd.h>
#include <pthread.h>

#include "comparator.h"
#include "config.h"
#include "iterator.h"
#include "standard_merge.h"
#include "standard_lookup.h"

template <class T>
struct StandardMergerArgs {
  Iterator<T> **iterators;
  int n;
  Comparator<T> *comparator;
  IteratorBuilder<T> *result;
};

template <class T>
class ParallelStandardMerger {
public:
  static Iterator<T> *merge(
    Iterator<T> *iter1, 
    Iterator<T> *iter2, 
    int num_threads,
    Comparator<T> *comparator, 
    IteratorBuilder<T> *result) {

    pthread_t threads[num_threads];
    StandardMergerArgs<T> *args[num_threads];

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

      Iterator<T> **iterators = new Iterator<T>*[2];
      iterators[0] = iter1_subrange; 
      iterators[1] = iter2_subrange;

      args[i] = new StandardMergerArgs<T>();
      args[i]->iterators = iterators;
      args[i]->n = 2;
      args[i]->comparator = comparator;
      args[i]->result = result_subrange;
      pthread_create(&threads[i], NULL, standard_merge, args[i]);

      iter1_start = iter1_end;
      iter2_start = iter2_end;
      result_start = result_end;
    }

    for (uint64_t i=0; i<num_threads; i++) {
       pthread_join(threads[i], NULL);
    }

    if (iter2_start != iter2->num_keys()) {
      Iterator<T> *iter1_subrange = iter1->subRange(iter1_start, iter1_start);
      Iterator<T> *iter2_subrange = iter2->subRange(iter2_start, iter2->num_keys());
      uint64_t result_end = result_start + (iter2->num_keys() - iter2_start);
      IteratorBuilder<T> *result_subrange = result->subRange(result_start, result_end);
      Iterator<T> *iterators[2] = {iter1_subrange, iter2_subrange};
      StandardMerger<T>::merge(iterators, 2, comparator, result_subrange);
    }
    return result->build();
  }
private:
  static void *standard_merge(void *a) {
    struct StandardMergerArgs<T> *args = (struct StandardMergerArgs<T> *)a;
    StandardMerger<T>::merge(args->iterators, args->n, args->comparator, args->result);
    return NULL;
  }
};
#endif

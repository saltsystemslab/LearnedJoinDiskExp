#ifndef MERGE_H
#define MERGE_H

#include "comparator.h"
#include "config.h"
#include "iterator.h"
#include <stdio.h>

class StandardMerger {
public:
  template <class T>
  static Iterator<T> *merge(Iterator<T> **iterators, int n,
                            Comparator<T> *comparator,
                            IteratorBuilder<T> *result) {
#if TRACK_STATS
    comparator = new CountingComparator<T>(comparator);
#endif
    printf("StandardMerge!\n");
    for (int i = 0; i < n; i++) {
      iterators[i]->seekToFirst();
    }
    Iterator<T> *smallest;
    while ((smallest = findSmallest(iterators, n, comparator)) != nullptr) {
      result->add(smallest->key());
      smallest->next();
    }
#if TRACK_STATS
    CountingComparator<T> *count_comp =
        dynamic_cast<CountingComparator<T> *>(comparator);
    std::cout << "CompCount: " << count_comp->get_count() << std::endl;
#endif
    return result->finish();
  }

private:
  template <class T>
  static Iterator<T> *findSmallest(Iterator<T> **iterators, int n,
                                   Comparator<T> *comparator) {
    Iterator<T> *smallest = nullptr;
    for (int i = 0; i < n; i++) {
      auto child = iterators[i];
      if (child->valid()) {
        if (smallest == nullptr) {
          smallest = child;
        } else if (comparator->compare(child->key(), smallest->key()) < 0) {
          smallest = child;
        }
      }
    }
    return smallest;
  }
};
#endif

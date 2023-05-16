#ifndef MERGE_H
#define MERGE_H

#include "comparator.h"
#include "iterator.h"
#include "merge.h"
#include "merge_result_builder.h"

template <class T>
Iterator<T> *findSmallest(Iterator<T> **iterators, int n,
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

template <class T>
void standardMerge(Iterator<T> **iterators, int n, Comparator<T> *comparator,
                   MergeResultBuilder<T> *resultBuilder) {
  Iterator<T> *smallest;
  while ((smallest = findSmallest(iterators, n, comparator)) != nullptr) {
    resultBuilder->add(smallest->key());
    smallest->next();
  }
}

#endif
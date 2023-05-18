#ifndef MERGE_H
#define MERGE_H

#include "comparator.h"
#include "iterator.h"

class StandardMerger {
public:
  template <class T>
  static void merge(Iterator<T> **iterators, int n, Comparator<T> *comparator,
                    IteratorBuilder<T> *result) {
    for (int i = 0; i < n; i++) {
      iterators[i]->seekToFirst();
    }
    Iterator<T> *smallest;
    while ((smallest = findSmallest(iterators, n, comparator)) != nullptr) {
      result->add(smallest->key());
      smallest->next();
    }
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

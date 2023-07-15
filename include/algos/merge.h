#ifndef LEARNEDINDEXMERGE_MERGE_H
#define LEARNEDINDEXMERGE_MERGE_H

#include "comparator.h"
#include "index.h"
#include "iterator.h"
#include "sstable.h"

namespace li_merge {

template <class T>
Iterator<T> *findSmallest(Iterator<T> **iterators, int n,
                          Comparator<T> *comparator);

template <class T>
SSTable<T> *standardMerge(SSTable<T> *outer_table, SSTable<T> *inner_table,
                          Comparator<T> *comparator,
                          SSTableBuilder<T> *builder) {
  Iterator<T> *inner_iter = inner_table->iterator();
  Iterator<T> *outer_iter = outer_table->iterator();
  inner_iter->seekToFirst();
  outer_iter->seekToFirst();

  Iterator<T> *iterators[2] = {inner_iter, outer_iter};
  Iterator<T> *smallest;
  while ((smallest = findSmallest(iterators, 2, comparator)) != nullptr) {
    builder->add(smallest->key());
    smallest->next();
  }
  return builder->build();
}

/*
template <class T>
int merge_with_indexes(SSTable<T> *outer_table, SSTable<T> *inner_table,
                       Index<T> *inner_index, Comparator<T> *comparator,
                       SSTableBuilder<T> *builder);
*/

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

} // namespace li_merge

#endif // LEARNEDINDEXMERGE_MERGE_H

#ifndef LEARNEDINDEXMERGE_MERGE_H
#define LEARNEDINDEXMERGE_MERGE_H

#include "comparator.h"
#include "index.h"
#include "iterator.h"
#include "sstable.h"

namespace li_merge {
template <class T>
int standard_merge(Iterator<T> *outer_iterator, Iterator<T> *inner_iterator,
                   Comparator<T> *comparator, SSTableBuilder<T> builder);

template <class T>
int merge_with_indexes(Iterator<T> *outer_iterator, Iterator<T> *inner_iterator,
                       Index<T> *inner_index, Comparator<T> *comparator,
                       SSTableBuilder<T> builder);
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_MERGE_H

#ifndef LEARNEDINDEXMERGE_JOIN_H
#define LEARNEDINDEXMERGE_JOIN_H

#include "comparator.h"
#include "index.h"
#include "iterator.h"
#include "sstable.h"

namespace li_merge {
template <class T>
int indexed_nested_loop_join(Iterator<T> *outer_iterator,
                             Iterator<T> *inner_iterator,
                             Iterator<T> *inner_index,
                             Comparator<T> *comparator,
                             SSTableBuilder<T> *result);

template <class T>
int presorted_merge_join(Iterator<T> *outer_iterator,
                         Iterator<T> *inner_iterator, Comparator<T> *comparator,
                         SSTableBuilder<T> *result_builder);

} // namespace li_merge

#endif
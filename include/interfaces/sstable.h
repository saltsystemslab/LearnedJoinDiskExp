#ifndef LEARNEDINDEXMERGE_SSTABLE_H
#define LEARNEDINDEXMERGE_SSTABLE_H

#include "iterator.h"
#include <stdlib.h>

namespace li_merge {
template <class T> class SSTable {
public:
  virtual Iterator<T> *iterator() = 0;
  virtual Iterator<T> *iterator(int kv_buffer_size, bool aligned) { return nullptr; }
  virtual WindowIterator<T> *windowIterator() { return nullptr; }
};

template <class T> class SSTableBuilder {
public:
  virtual void add(const T &t) = 0;
  virtual SSTable<T> *build() = 0;
};

template <class T> class PSSTableBuilder {
public:
  virtual SSTableBuilder<T> *getBuilderForRange(uint64_t start_index,
                                                uint64_t end_index) = 0;
  virtual SSTable<T> *build() = 0;
};
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_SSTABLE_H

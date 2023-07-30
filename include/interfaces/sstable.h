#ifndef LEARNEDINDEXMERGE_SSTABLE_H
#define LEARNEDINDEXMERGE_SSTABLE_H

#include "iterator.h"
namespace li_merge {
template <class T> class SSTable {
public:
  virtual Iterator<T> *iterator() = 0;
};

template <class T> class SSTableBuilder {
public:
  virtual void add(const T &t) = 0;
  virtual SSTable<T> *build() = 0;
};

template <class T> class PSSTableBuilder {
public:
  virtual SSTableBuilder<T> *getBuilderForRange(uint64_t offset_index) = 0;
  virtual SSTable<T> *build() = 0;
};
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_SSTABLE_H

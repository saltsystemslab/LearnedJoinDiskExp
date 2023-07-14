#ifndef LEARNEDINDEXMERGE_SSTABLE_H
#define LEARNEDINDEXMERGE_SSTABLE_H

#include "iterator.h"
namespace li_merge {
template <class T> class SSTable {
public:
  virtual Iterator<T> iterator() = 0;
  virtual Iterator<T> iterator(uint64_t start, uint64_t end) = 0;
};

template <class T> class SSTableBuilder {
public:
  virtual void add(const T &t) = 0;
  virtual SSTable<T> build() = 0;
};
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_SSTABLE_H

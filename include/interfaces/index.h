#ifndef LEARNEDINDEXMERGE_INDEX_H
#define LEARNEDINDEXMERGE_INDEX_H

#include "index.h"
#include "iterator.h"
#include "sstable.h"
#include <unistd.h>
#include <stdlib.h>

namespace li_merge {
struct Bounds {
  uint64_t lower;
  uint64_t upper;
  uint64_t approx_pos;
};

template <class T> class Index {
public:
  virtual Bounds getPositionBounds(const T &t) = 0;
  virtual Bounds getPositionBoundsRA(const T &t) = 0;
  virtual uint64_t sizeInBytes() = 0;
  virtual uint64_t getMaxError() {abort();};
  virtual bool isErrorPageAligned() {abort();};
  virtual Index<T> *shallow_copy() {return this;};
};

template <class T> class IndexBuilder {
public:
  virtual void add(const T &t) = 0;
  virtual Index<T> *build() = 0;
};

template <class T>
Index<T> *buildIndexFromIterator(Iterator<T> *iterator,
                                 IndexBuilder<T> *builder) {
  iterator->seekToFirst();
  while (iterator->valid()) {
    builder->add(iterator->key());
    iterator->next();
  }
  return builder->build();
}

template <class T>
Index<T> *buildIndex(SSTable<T> *table, IndexBuilder<T> *builder) {
  Iterator<T> *iter = table->iterator();
  Index<T> *index = buildIndexFromIterator(iter, builder);
  delete iter;
  return index;
}

} // namespace li_merge

#endif // LEARNEDINDEXMERGE_INDEX_H

#ifndef LEARNEDINDEXMERGE_INDEX_H
#define LEARNEDINDEXMERGE_INDEX_H

#include "index.h"
#include "iterator.h"
#include "sstable.h"
#include <unistd.h>

namespace li_merge {
struct Bounds {
  uint64_t lower;
  uint64_t upper;
  uint64_t approx_pos;
};

template <class T> class Index {
public:
  virtual uint64_t getApproxPosition(const T &t) = 0;
  virtual uint64_t getApproxLowerBoundPosition(const T &t) = 0;
  virtual Bounds getPositionBounds(const T &t) = 0;
  virtual uint64_t getApproxPositionMonotoneAccess(const T &t) = 0;
  virtual uint64_t getApproxLowerBoundPositionMonotoneAccess(const T &t) = 0;
  virtual Bounds getPositionBoundsMonotoneAccess(const T &t) = 0;
  virtual void resetMonotoneAccess() = 0;
  virtual uint64_t size_in_bytes() = 0;
  virtual Index<T> *getIndexForSubrange(uint64_t start, uint64_t end) = 0;
};

/* Exact Index with Bulk Load. Mainly a wrapper around B+ Tree. */
template <class T> class BTreeIndex {
public:
  virtual bool exists(const T &t);
  virtual void bulkLoad();
  virtual BTreeIndex<T> *getIndexForRange(uint64_t s, uint64_t e);
};

template <class T> class IndexBuilder {
  public:
  virtual void add(const T &t) = 0;
  virtual Index<T> *build() = 0;
};

template <class T>
Index<T> *build_index_from_iterator(Iterator<T> *iterator,
                                    IndexBuilder<T> *builder) {
  iterator->seekToFirst();
  while (iterator->valid()) {
    builder->add(iterator->key());
    iterator->next();
  }
  return builder->build();
}

template <class T>
Index<T> *build_index(SSTable<T> *table, IndexBuilder<T> *builder) {
  Iterator<T> *iter = table->iterator();
  Index<T> *index = build_index_from_iterator(iter, builder);
  delete iter;
  return index;
}

} // namespace li_merge

#endif // LEARNEDINDEXMERGE_INDEX_H

#ifndef LEARNEDINDEXMERGE_INDEX_H
#define LEARNEDINDEXMERGE_INDEX_H

#include "iterator.h"
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
  virtual Bounds getPositionBounds(const T &t) = 0;
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

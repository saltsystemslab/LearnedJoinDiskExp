#ifndef MERGE_RESULT_BUILDER_H
#define MERGE_RESULT_BUILDER_H

template <class T> class MergeResultBuilder {
public:
  virtual void add(T t) = 0;
};

#endif

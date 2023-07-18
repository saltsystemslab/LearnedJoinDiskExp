#ifndef LEARNEDINDEXMERGE_COMPARATOR_H
#define LEARNEDINDEXMERGE_COMPARATOR_H

#include <stdint.h>

namespace li_merge {
template <class T> class Comparator {
public:
  virtual int compare(const T &first_key, const T &sec_key) = 0;
};

template <class T> class CountingComparator : public Comparator<T> {
public:
  CountingComparator(Comparator<T> *comparator)
      : comparator_(comparator), comp_count_(0) {}
  int compare(const T &first_key, const T &sec_key) override {
    comp_count_++;
    return comparator_->compare(first_key, sec_key);
  }
  uint64_t get_count() { return comp_count_; }

private:
  Comparator<T> *comparator_;
  uint64_t comp_count_;
};

} // namespace li_merge

#endif // LEARNEDINDEXMERGE_COMPARATOR_H

#ifndef LEARNEDINDEXMERGE_COMPARATOR_H
#define LEARNEDINDEXMERGE_COMPARATOR_H

namespace li_merge {
template <class T> class Comparator {
public:
  virtual int compare(const T &first_key, const T &sec_key) = 0;
};
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_COMPARATOR_H

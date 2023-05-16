#ifndef COMPARATOR_H
#define COMPARATOR_H

template <class T> class Comparator {
public:
  virtual int compare(const T &first_key, const T &sec_key) const = 0;
};
#endif

#ifndef COMPARATOR_H
#define COMPARATOR_H

template <class T>
class Comparator {
 public:
  virtual int compare(const T &first_key, const T &sec_key) = 0;
};

template <class T>
class CountingComparator : public Comparator<T> {
 public:
  CountingComparator(Comparator<T> *c) : c_(c), count_(0) {}

  int compare(const T &first_key, const T &sec_key) {
    count_++;
    return c_->compare(first_key, sec_key);
  };

  uint64_t get_count() { return count_; }

 private:
  uint64_t count_;
  Comparator<T> *c_;
};

#endif

#ifndef BINARY_SEARCH_H
#define BINARY_SEARCH_H

#include "plr.h"
#include "slice.h"
#include "model.h"
#include "iterator.h"
#include "comparator.h"
#include "slice_comparator.h"

class BinarySearch : public Model<Slice> {
public:
  BinarySearch(std::vector<Slice> *a):
  a_(a)
  {}
  double guessPositionMonotone(Slice target_key) override;
  double guessPositionUsingBinarySearch(Slice target_key) override;
private:
  std::vector<Slice> *a_;
  double binary_search(int64_t start, int64_t end, Slice target_key, Comparator<Slice> *comparator);
  
};

class BinarySearchBuilder : public ModelBuilder<Slice> {
public:
  BinarySearchBuilder(int n)
  {
    this->a_ = new std::vector<Slice>(n);
    this->n_ = n;
    this->cur_ = 0;
  }
  void add(const Slice &t) override {
   // a_.push_back(t);
   (*a_)[cur_++] = t;
    
  }
  BinarySearch* finish() override {
    return new BinarySearch(a_);
  }

private:
  std::vector<Slice> *a_;
  int n_;
  int cur_;
};


#endif

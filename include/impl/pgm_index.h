#ifndef PGM_INDEX_H
#define PGM_INDEX_H

#include "pgm/pgm_index.hpp"
#include "slice.h"
#include "model.h"
#include "iterator.h"
#include "config.h"
#include <iostream>
static const int epsilon = PLR_ERROR_BOUND;

class PGMIndex : public Model<Slice> {
public:
  PGMIndex(pgm::PGMIndex<KEY_TYPE, epsilon> *(a)):
  a_(a)
  {}
  double guessPositionMonotone(Slice target_key) override;
  double guessPositionUsingBinarySearch(Slice target_key) override;
  uint64_t getNumberOfSegments() override;
private:
  pgm::PGMIndex<KEY_TYPE, epsilon> *(a_);
  
};

class PGMIndexBuilder : public ModelBuilder<Slice> {
public:
  PGMIndexBuilder(int n)
  {
    this->a_ = new std::vector<KEY_TYPE>(n);
    this->n_ = n;
    this->cur_ = 0;
  }
  void add(const Slice &t) override {
    KEY_TYPE *k = (KEY_TYPE *) t.data_;
    
     (*a_)[cur_++] = *k;

}
  PGMIndex* finish() override {
    return new PGMIndex(new pgm::PGMIndex<KEY_TYPE, epsilon>(*a_));
  }

private:
  std::vector<KEY_TYPE> *a_;
  int n_;
  int cur_;
};


#endif

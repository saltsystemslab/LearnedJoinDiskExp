#ifndef SLICE_PGM_INDEX_H
#define SLICE_PGM_INDEX_H

#include "pgm/pgm_index.hpp"
#include "slice.h"
#include "model.h"
#include "iterator.h"
#include "config.h"

static const int epsilon = 10;

class SlicePGMIndex : public Model<Slice> {
public:
  SlicePGMIndex(pgm::PGMIndex<KEY_TYPE, epsilon> *(a)):
  a_(a)
  {}
  uint64_t guessPositionMonotone(Slice target_key) override {
    KEY_TYPE *k = (KEY_TYPE *) target_key.data_;
    return a_->search(*k).pos;
  }

  uint64_t guessPosition(Slice target_key) override {
    KEY_TYPE *k = (KEY_TYPE *) target_key.data_;
    return a_->search(*k).pos;
  }
  SlicePGMIndex *get_model_for_subrange(uint64_t start, uint64_t end) override {
    return new SlicePGMIndex(a_);  //incorrect
  }
	double getMaxError() override {
		0;  //incorrect
	}
	uint64_t size_bytes() override {
		return a_->size_in_bytes();
	}
private:
  pgm::PGMIndex<KEY_TYPE, epsilon> *(a_);

};

class SlicePGMIndexBuilder : public ModelBuilder<Slice> {
public:
  SlicePGMIndexBuilder(int n)
  {
    this->a_ = new std::vector<KEY_TYPE>(n);
    this->n_ = n;
    this->cur_ = 0;
  }
  void add(const Slice &t) override {
    KEY_TYPE *k = (KEY_TYPE *) t.data_;
     (*a_)[cur_++] = *k;
}
  SlicePGMIndex* finish() override {
    return new SlicePGMIndex(new pgm::PGMIndex<KEY_TYPE, epsilon>(*a_));
  }

private:
  std::vector<KEY_TYPE> *a_;
  int n_;
  int cur_;
};


#endif
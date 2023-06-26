#ifndef SLICE_PGM_INDEX_H
#define SLICE_PGM_INDEX_H

#include "pgm/pgm_index.hpp"
#include "slice.h"
#include "model.h"
#include "iterator.h"
#include "config.h"

//static const int epsilon = PGM_ERROR_BOUND;

class SlicePGMIndex : public Model<Slice> {
public:
  SlicePGMIndex(pgm::PGMIndex<uint64_t, epsilon> *(a)):
  a_(a)
  {}
  uint64_t guessPositionMonotone(Slice target_key) override {
   // KEY_TYPE *k = (KEY_TYPE *) target_key.data_;
    return a_->search(target_key.to_uint64()).pos;
  }

  uint64_t guessPosition(Slice target_key) override {
   //KEY_TYPE *k = (KEY_TYPE *) target_key.data_;
    return a_->search(target_key.to_uint64()).pos;
  }
  SlicePGMIndex *get_model_for_subrange(uint64_t start, uint64_t end) override {
    return new SlicePGMIndex(a_);  //incorrect
  }
	double getMaxError() override {
		return epsilon;  //incorrect
	}
	uint64_t size_bytes() override {
		return a_->size_in_bytes();
	}
private:
  pgm::PGMIndex<uint64_t, epsilon> *(a_);

};

class SlicePGMIndexBuilder : public ModelBuilder<Slice> {
public:
  SlicePGMIndexBuilder(int n)
  {
    this->a_ = new std::vector<uint64_t>(n);
    this->n_ = n;
    this->cur_ = 0;
  }
  void add(const Slice &t) override {
    
     (*a_)[cur_++] = t.to_uint64();
}
  SlicePGMIndex* finish() override {
    return new SlicePGMIndex(new pgm::PGMIndex<uint64_t, epsilon>(*a_));
  }

private:
  std::vector<uint64_t> *a_;
  int n_;
  int cur_;
};


#endif
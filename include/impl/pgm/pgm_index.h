#ifndef PGM_INDEX_H
#define PGM_INDEX_H

#include "pgm/pgm_index.hpp"
#include "slice.h"
#include "model.h"
#include "iterator.h"
#include "config.h"

static const int epsilon = PGM_ERROR_BOUND;

template <class T>
class IntPGMIndex : public Model<T> {
public:
  IntPGMIndex(pgm::PGMIndex<KEY_TYPE, epsilon> *(a)):
  a_(a)
  {}
  uint64_t guessPositionMonotone(T target_key) override {
    KEY_TYPE *k = (KEY_TYPE *) target_key.data_;
    return a_->search(*k).pos;
  }

  uint64_t guessPosition(T target_key) override {
    KEY_TYPE *k = (KEY_TYPE *) target_key.data_;
    return a_->search(*k).pos;
  }
  Model<T> *get_model_for_subrange(const T &start, const T &end) override {
    return new IntPGMIndex(a_);  //incorrect
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

template <class T>
class IntPGMIndexBuilder : public ModelBuilder<T> {
public:
  IntPGMIndexBuilder(int n)
  {
    this->a_ = new std::vector<KEY_TYPE>(n);
    this->n_ = n;
    this->cur_ = 0;
  }
  void add(const T &t) override {
    KEY_TYPE *k = (KEY_TYPE *) t.data_;
     (*a_)[cur_++] = *k;
}
  PGMIndex* finish() override {
    return new IntPGMIndex(new pgm::PGMIndex<KEY_TYPE, epsilon>(*a_));
  }

private:
  std::vector<KEY_TYPE> *a_;
  int n_;
  int cur_;
};


#endif
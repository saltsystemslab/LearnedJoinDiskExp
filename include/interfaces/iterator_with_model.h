#ifndef ITERATOR_WITH_MODEL
#define ITERATOR_WITH_MODEL

#include "iterator.h"
#include "model.h"
#include "slice.h"
#include "slice_iterator.h"

#include "plr.h"

template <class T>
class IteratorWithModel : public Iterator<T>, public Model<T> {
public:
  IteratorWithModel(Iterator<T> *iterator, Model<T> *model)
      : iterator_(iterator), model_(model){};
  bool valid() const override { return iterator_->valid(); }
  void next() override { iterator_->next(); }
  T peek(uint64_t pos) const override { return iterator_->peek(pos); }
  void seek(Slice item) override { iterator_->seek(item); }
  void seekToFirst() override { iterator_->seekToFirst(); }
  T key() override { return iterator_->key(); }
  uint64_t current_pos() const override { return iterator_->current_pos(); }
  uint64_t num_keys() const { return iterator_->num_keys(); }
  double guessPositionMonotone(T target_key) {return model_->guessPositionMonotone(target_key);};
  double guessPositionUsingBinarySearch(T target_key) {return model_->guessPositionUsingBinarySearch(target_key);};
  uint64_t bulkReadAndForward(uint64_t num_keys, char **data, uint64_t *len) {
    return iterator_->bulkReadAndForward(num_keys, data, len);
  };
  uint64_t getNumberOfSegments() { 
     return model_->getNumberOfSegments();
  };

private:
  Model<T> *model_;
  Iterator<T> *iterator_;
};

template <class T>
class IteratorWithModelBuilder {
public:
  size_t training_time_;
  IteratorWithModelBuilder(IteratorBuilder<T>* iterator, ModelBuilder<T>* model)
      :
      iterator_(iterator),
      model_(model),
      training_time_(0)
      {}

  void add(const T &key) {
    iterator_->add(key);

    auto training_time_start = std::chrono::high_resolution_clock::now();
    model_->add(key);
    auto training_time_stop = std::chrono::high_resolution_clock::now();
    training_time_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                       training_time_stop - training_time_start)
                       .count();
  };
  IteratorWithModel<T> *finish() {
    Iterator<T> *it = iterator_->finish();

    auto training_time_start = std::chrono::high_resolution_clock::now();
    Model<T>* m = model_->finish();
    auto training_time_stop = std::chrono::high_resolution_clock::now();
    training_time_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                       training_time_stop - training_time_start)
                       .count();
    std::cout<<"Training duration: "<<training_time_<<std::endl;
    return new IteratorWithModel(it, m);
  };

private:
  IteratorBuilder<T>* iterator_;
  ModelBuilder<T>* model_;
  
};
#endif
#ifndef ITERATOR_WITH_MODEL
#define ITERATOR_WITH_MODEL

#include "iterator.h"
#include "model.h"
#include "slice.h"
#include "slice_iterator.h"

#include "plr.h"

template <class T>
class IteratorWithModel : public Iterator<T>, Model<T> {
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

private:
  Model<T> *model_;
  Iterator<T> *iterator_;
};

template <class T>
class IteratorWithModelBuilder {
public:
  IteratorWithModelBuilder(IteratorBuilder<T>* iterator, ModelBuilder<T>* model)
      :
      iterator_(iterator),
      model_(model){}

  void add(const T &key) {
    iterator_->add(key);
    model_->add(key);
  };
  IteratorWithModel<T> *finish() {
    return new IteratorWithModel(iterator_->finish(),
                                      model_->finish());
  };

private:
  IteratorBuilder<T>* iterator_;
  ModelBuilder<T>* model_;
};
#endif
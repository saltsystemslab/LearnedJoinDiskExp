#ifndef ITERATOR_WITH_MODEL
#define ITERATOR_WITH_MODEL

#include "iterator.h"
#include "model.h"
#include "plr.h"

template <class T> class IteratorWithModel {
public:
  IteratorWithModel(Iterator<T> *iterator, Model<T> *model)
      : iterator_(iterator), model_(model), num_keys_(iterator->num_keys()){};
  bool valid() const { return iterator_->valid(); }
  void next() { iterator_->next(); }
  T peek(uint64_t pos) const { return iterator_->peek(pos); }
  void seekToFirst() { iterator_->seekToFirst(); }
  T key() { return iterator_->key(); }
  uint64_t current_pos() const { return iterator_->current_pos(); }
  uint64_t num_keys() const { return iterator_->num_keys(); }
  uint64_t guessPositionMonotone(T target_key) {
    return model_->guessPositionMonotone(target_key);
  };
  uint64_t guessPosition(T target_key) {
    return model_->guessPosition(target_key);
  };
  uint64_t bulkReadAndForward(uint64_t num_keys, char **data, uint64_t *len) {
    return iterator_->bulkReadAndForward(num_keys, data, len);
  };
  Iterator<T> *get_iterator() { return iterator_; }
  IteratorWithModel<T> *subRange(uint64_t start, uint64_t end) {
    return new IteratorWithModel<T>(iterator_->subRange(start, end),
                                    model_->get_model_for_subrange(start, end));
  }
  std::string id() { return iterator_->id(); }
  double getMaxError() { return model_->getMaxError(); }
  uint64_t model_size_bytes() { return model_->size_bytes(); }

private:
  Model<T> *model_;
  Iterator<T> *iterator_;
  uint64_t num_keys_;
};

template <class T> class IteratorWithModelBuilder {
public:
  IteratorWithModelBuilder(IteratorBuilder<T> *iterator, ModelBuilder<T> *model)
      : iterator_(iterator), model_(model) {}

  void add(const T &key) {
    iterator_->add(key);
    model_->add(key);
  };
  IteratorWithModel<T> *finish() {
    return new IteratorWithModel<T>(iterator_->build(), model_->finish());
  };

private:
  IteratorBuilder<T> *iterator_;
  ModelBuilder<T> *model_;
};
#endif

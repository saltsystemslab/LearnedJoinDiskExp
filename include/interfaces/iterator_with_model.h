#ifndef ITERATOR_WITH_MODEL
#define ITERATOR_WITH_MODEL

#include "iterator.h"
#include "model.h"
#include "plr.h"

template <class T>
class IteratorWithModel {
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
    double guessPositionMonotone(T target_key) {
	return model_->guessPositionMonotone(target_key);
    };
    double guessPosition(T target_key) {
	return model_->guessPosition(target_key);
    };
    uint64_t bulkReadAndForward(uint64_t num_keys, char **data, uint64_t *len) {
	return iterator_->bulkReadAndForward(num_keys, data, len);
    };
    Iterator<T> *get_iterator() { return iterator_; }
    IteratorWithModel<T> *subRange(uint64_t start, uint64_t end) {
	return new IteratorWithModel<T>(
	    iterator_->subRange(start, end),
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

template <class T>
class IteratorWithModelBuilder {
   public:
   size_t training_time_;
    IteratorWithModelBuilder(IteratorBuilder<T> *iterator,
			     ModelBuilder<T> *model)
	: iterator_(iterator), model_(model), training_time_(0) {}

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
    Iterator<T> *it = iterator_->build();

    auto training_time_start = std::chrono::high_resolution_clock::now();
    Model<T>* m = model_->finish();
    auto training_time_stop = std::chrono::high_resolution_clock::now();
    training_time_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                       training_time_stop - training_time_start)
                       .count();
    float training_duration_sec = training_time_ / 1e9;
    printf("Training duration: %.3lf s\n", training_duration_sec);
    return new IteratorWithModel(it, m);
	//return new IteratorWithModel(iterator_->build(), model_->finish());
    };

   private:
    IteratorBuilder<T> *iterator_;
    ModelBuilder<T> *model_;
};
#endif

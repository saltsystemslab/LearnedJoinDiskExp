#ifndef INT_ARRAY_H
#define INT_ARRAY_H

#include "iterator.h"

template <class T>
class IntArrayIterator : public Iterator<T> {
 public:
  IntArrayIterator(T *a, uint64_t num_keys, std::string id)
      : id_(id), num_keys_(num_keys), cur_(0), a_(a) {}
  ~IntArrayIterator() { delete a_; }
  bool valid() const override { return cur_ < num_keys_; };
  void next() override { cur_++; };
  T peek(uint64_t pos) const override { return a_[pos]; };
  void seekToFirst() override { cur_ = 0; };
  T key() override { return a_[cur_]; };
  uint64_t current_pos() const override { return cur_; };
  std::string id() override { return id_; }
  uint64_t num_keys() const override { return num_keys_; }
  virtual uint64_t bulkReadAndForward(uint64_t num_keys_to_read, char **data,
                                      uint64_t *len) {
    *data = (char *)(a_ + cur_);
    *len = num_keys_to_read * sizeof(T);
    cur_ += num_keys_to_read;
    return num_keys_to_read;
  };

 private:
  T *a_;
  uint64_t num_keys_;
  uint64_t cur_;
  std::string id_;
};

template <class T>
class IntArrayBuilder : public IteratorBuilder<T> {
 public:
  IntArrayBuilder(uint64_t num_keys, std::string id)
      : a_(new T[num_keys]),
        cur_(0),
        num_keys_(num_keys),
        id_(id),
        finished_(false){};
  void add(const T &t) override { a_[cur_++] = t; }
  Iterator<T> *build() override {
    return new IntArrayIterator<T>(a_, num_keys_, id_);
  };
  void bulkAdd(Iterator<T> *iter, uint64_t keys_to_add) override {
    char *buffer;
    uint64_t len;
    while (keys_to_add > 0) {
      uint64_t keys_added =
          iter->bulkReadAndForward(keys_to_add, &buffer, &len);
      keys_to_add -= keys_added;
      memcpy(a_ + cur_, buffer, len);
      cur_ += keys_added;
    }
  }

 private:
  T *a_;
  uint64_t num_keys_;
  uint64_t cur_;
  std::string id_;
  bool finished_;
};

#endif

#ifndef LEARNEDINDEXMERGE_ITERATOR_H
#define LEARNEDINDEXMERGE_ITERATOR_H

namespace li_merge {
template <class T> class Iterator {
  virtual bool valid() const = 0;
  virtual void next() = 0;
  virtual T peek(uint64_t pos) const = 0;
  virtual void seekToFirst() = 0;
  virtual void seekToPos(uint64_t pos) = 0;
  virtual T key() = 0;
  virtual uint64_t current_pos() const = 0;
  virtual uint64_t num_keys() const = 0;
  virtual std::string id() { return "unnamed_iterator"; }
};
} // namespace li_merge

#endif // LEARNEDINDEXMERGE_ITERATOR_H

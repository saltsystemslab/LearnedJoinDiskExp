#ifndef SLICE_H
#define SLICE_H

class Slice {
public:
  Slice() : data(""), size(0) {}
  Slice(const char *data, int len) : data_(data), size_(len);
  const char *data_;
  size_t size_;
};

#endif

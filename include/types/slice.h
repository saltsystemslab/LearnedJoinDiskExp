#ifndef SLICE_H
#define SLICE_H

#include <cstddef>
#include <string>

class Slice {
public:
  Slice() : data_(""), size_(0) {}
  Slice(const char *data, int len) : data_(data), size_(len){};
  const char *data_;
  size_t size_;

  std::string toString() const {
    char str[2 * size_ + 1];
    for (int i = 0; i < size_; i++) {
      uint8_t c = *(data_ + i);
      sprintf(str + 2 * i, "%02X", c);
    }
    str[2 * size_] = '\0';
    return std::string(str, 2 * size_ + 1);
  }
};

#endif

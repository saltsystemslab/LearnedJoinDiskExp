#include "iterator.h"
#include "slice.h"
#include "slice_file_iterator.h"
#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <unistd.h>

int main(int argc, char **argv) {
  FixedSizeSliceFileIteratorBuilder builder("./test.txt", 100, 6);

  const char *string_1 = "Hello,";
  const char *string_2 = "World!";

  builder.add(Slice(string_1, strlen(string_1)));
  builder.add(Slice(string_2, strlen(string_2)));

  Iterator<Slice> *result = builder.finish();

  int fd = open("./test.txt", O_RDONLY);
  char buffer[7];
  buffer[6] = '\0';

  result->seekToFirst();
  while (result->valid()) {
    Slice s = result->key();
    result->next();
    for (int i=0; i<s.size_; i++) {
      printf("%c", s.data_[i]);
    }
    printf("\n");
  }

  assert(true);
  std::cout << "Ok!" << std::endl;
}

#include "iterator.h"
#include "slice.h"
#include "slice_file_iterator.h"
#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <iostream>

int main(int argc, char **argv) {
  FixedSizeSliceFileIteratorBuilder builder("./test.txt", 100, 6);

  const char *string_1 = "Hello,";
  const char *string_2 = "World!";

  builder.add(Slice(string_1, strlen(string_1)));
  builder.add(Slice(string_2, strlen(string_2)));

  builder.finish();

  assert(true);
  std::cout << "Ok!" << std::endl;
}

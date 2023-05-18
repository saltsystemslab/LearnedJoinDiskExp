#include "iterator.h"
#include "slice.h"
#include "slice_file_iterator.h"
#include <cassert>
#include <iostream>
#include <fcntl.h>
#include <cstring>

int main(int argc, char **argv) {
  SliceFileIteratorBuilder builder("./test.txt", 100);

  const char * string_1 = "Hello,";
  const char * string_2 = "World!";

  builder.add(Slice(string_1, strlen(string_1)));
  builder.add(Slice(string_2, strlen(string_2)));

  builder.finish();

  assert(true);
}

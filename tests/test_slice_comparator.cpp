#include "comparator.h"
#include "iterator.h"
#include "slice_array.h"
#include "slice_comparator.h"
#include "standard_merge.h"
#include <cassert>
#include <iostream>

#define KEY_SIZE 19
using namespace std;

string generate_key(int key_value) {
  string key = to_string(key_value);
  string result = string(KEY_SIZE - key.length(), '0') + key;
  return std::move(result);
}

int main() {

  SliceArrayBuilder *builder1 = new SliceArrayBuilder(10, KEY_SIZE);
  for (int i = 0; i < 10; i++) {
    std::string key = generate_key(2 * i);
    builder1->add(Slice(key.c_str(), KEY_SIZE));
  }
  SliceArrayBuilder *builder2 = new SliceArrayBuilder(10, KEY_SIZE);
  for (int i = 0; i < 10; i++) {
    builder2->add(Slice(generate_key((2 * i + 1)).c_str(), KEY_SIZE));
  }

  Iterator<Slice> **iterators = new Iterator<Slice> *[2];
  iterators[0] = builder1->finish();
  iterators[1] = builder2->finish();

  while (iterators[0]->valid()) {
    iterators[0]->next();
  }

  while (iterators[1]->valid()) {
    iterators[1]->next();
  }
  Comparator<Slice> *c = new SliceComparator();

  SliceArrayBuilder *resultBuilder = new SliceArrayBuilder(20, KEY_SIZE);
  Iterator<Slice> *result =
      StandardMerger::merge(iterators, 2, c, resultBuilder);

  int i = 0;
  while (result->valid()) {
    assert(c->compare(result->key(),
                      Slice(generate_key((i)).c_str(), KEY_SIZE)) == 0);
    i++;
    result->next();
  }
  assert(i == 20);
  std::cout << "Ok!" << std::endl;
}

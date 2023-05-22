#include "comparator.h"
#include "int_iterator.h"
#include "iterator.h"
#include "standard_merge.h"
#include <cassert>
#include <iostream>

using namespace std;


int main() {
  IntArrayIteratorBuilder *builder1 = new IntArrayIteratorBuilder(10);
  for (int i = 0; i < 10; i++) {
    builder1->add(2 * i);
  }
  IntArrayIteratorBuilder *builder2 = new IntArrayIteratorBuilder(10);
  for (int i = 0; i < 10; i++) {
    builder2->add(2 * i + 1);
  }

  Iterator<int> **iterators = new Iterator<int> *[2];
  iterators[0] = builder1->finish();
  iterators[1] = builder2->finish();

  Comparator<int> *c = new IntComparator();

  IntArrayIteratorBuilder *resultBuilder = new IntArrayIteratorBuilder(20);
  Iterator<int> *result = StandardMerger::merge(iterators, 2, c, resultBuilder);

  int i = 0;
  while (result->valid()) {
    assert(result->key() == i++);
    result->next();
  }
  assert(i == 20);
  std::cout << "Ok!" << std::endl;
}

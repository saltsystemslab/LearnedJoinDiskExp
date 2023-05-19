#include "slice_array_iterator.h"
#include "slice_comparator.h"
#include "math.h"

SliceArrayIterator::SliceArrayIterator(char *a, int n, int key_size)
{
  this->a = a;
  this->cur = 0;
  this->n = n;
  this->key_size = key_size;
}

#if LEARNED_MERGE
SliceArrayIterator::SliceArrayIterator(char *a, int n, int key_size, PLRModel* model)
{
  this->a = a;
  this->cur = 0;
  this->n = n;
  this->key_size = key_size;
  this->model = model;
}
#endif
SliceArrayIterator::~SliceArrayIterator() { delete a; }
bool SliceArrayIterator::valid() const { return cur < n; }
void SliceArrayIterator::next()
{
  assert(valid());
  cur++;
}
Slice SliceArrayIterator::peek(uint64_t pos) const
{
  return Slice(a + pos * key_size, key_size);
}
void SliceArrayIterator::seek(Slice item)
{
  Comparator<Slice> *c = new SliceComparator();
  for (int i = 0; i < n; i++)
  {
    if (c->compare(Slice(a + i * key_size, key_size), item) > 0)
    {
      cur = i;
      return;
    }
  }
  cur = n;
}
void SliceArrayIterator::seekToFirst() { cur = 0; }
Slice SliceArrayIterator::key() const
{
  return Slice(a + cur * key_size, key_size);
}
uint64_t SliceArrayIterator::current_pos() const { return cur; }

#if LEARNED_MERGE
uint64_t SliceArrayIterator::getPLRLineSegmentIndex()
{
  return plr_segment_index;
}

void SliceArrayIterator::setPLRLineSegmentIndex(uint64_t value)
{
  plr_segment_index = value;
}
double SliceArrayIterator::guessPosition(Slice target_key)
{
  std::vector<Segment> &segments = model->lineSegments_;
  uint64_t target_int = LdbKeyToInteger(target_key.toString());

  for (uint64_t i = getPLRLineSegmentIndex(); i < (uint64_t)segments.size(); i++)
  {
    if (segments[i].last > target_int)
    {
      setPLRLineSegmentIndex(i);
      double result = target_int * segments[i].k + segments[i].b;
      if (result < 0)
      {
        result = 0;
      }
      return floor(result);
    }
  }
  return target_int * segments[(uint64_t)segments.size()-1].k + segments[(uint64_t)segments.size()-1].b;
}
#endif
SliceArrayBuilder::SliceArrayBuilder(int n, int key_size)
{
  this->a = new char[n * key_size];
  this->cur = 0;
  this->n = n;
  this->key_size = key_size;
  #if LEARNED_MERGE
  plrBuilder = new PLRBuilder(10);
  #endif
}
void SliceArrayBuilder::add(const Slice &t)
{
  for (int i = 0; i < key_size; i++)
  {
    a[cur * key_size + i] = t.data_[i];
  }
  cur++;
  #if LEARNED_MERGE
  plrBuilder->processKey(LdbKeyToInteger(t.toString()));
  #endif
}
SliceArrayIterator *SliceArrayBuilder::finish()
{
  return new SliceArrayIterator(a, n, key_size);
  #if LEARNED_MERGE
  return new SliceArrayIterator(a, n, key_size, plrBuilder->finishTraining());
  #endif
}

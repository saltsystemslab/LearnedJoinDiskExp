#include "slice_iterator.h"

#include "config.h"
#include "math.h"

SliceIterator::SliceIterator() : model(nullptr), plr_segment_index(0) {}
uint64_t SliceIterator::getPLRLineSegmentIndex() { return plr_segment_index; }

void SliceIterator::setPLRLineSegmentIndex(uint64_t value)
{
  plr_segment_index = value;
}
double SliceIterator::guessPosition(Slice target_key)
{
  std::vector<Segment> &segments = model->lineSegments_;
#if USE_STRING_KEYS
  KEY_TYPE target_int_value = LdbKeyToInteger(target_key);
  KEY_TYPE *target_int = &target_int_value;
#else
  KEY_TYPE *target_int = (KEY_TYPE *)target_key.data_;
#endif
  for (uint64_t i = getPLRLineSegmentIndex(); i < (uint64_t)segments.size();
       i++)
  {
    if (segments[i].last > *target_int)
    {
      setPLRLineSegmentIndex(i);
      double result =
          (*target_int) * segments[i].k + segments[i].b - PLR_ERROR_BOUND;
      if (result < 0)
      {
        result = 0;
      }
      if (result >= num_keys_)
      {
        result = num_keys_ - 1;
      }
      return result;
    }
  }
  return num_keys_ - 1;
}

double SliceIterator::guessPositionUsingBinarySearch(Slice target_key)
{
  std::vector<Segment> &segments = model->lineSegments_;
#if USE_STRING_KEYS
  KEY_TYPE target_int_value = LdbKeyToInteger(target_key);
  KEY_TYPE *target_int = &target_int_value;
#else
  KEY_TYPE *target_int = (KEY_TYPE *)target_key.data_;
#endif

  uint32_t left = 0, right = (uint32_t)segments.size() - 1;
  while (left != right - 1)
  {

    uint32_t mid = (right + left) / 2;
    if ((*target_int) < segments[mid].x)
      right = mid;
    else
      left = mid;
  }

  double result =
      (*target_int) * segments[left].k + segments[left].b;
  if (result < 0)
  {
    result = 0;
  }
  return floor(result);
}

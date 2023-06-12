//#include "_iterator_with_model.h"
#include "plr_model.h"
#include "config.h"
#include "math.h"
#include <iostream>

uint64_t PLR_Model::getPLRLineSegmentIndex() {
  return plr_segment_index_;
}

void PLR_Model::setPLRLineSegmentIndex(uint64_t value) {
  plr_segment_index_ = value;
}
double PLR_Model::guessPositionMonotone(Slice target_key) {
  std::vector<Segment> &segments = model_->lineSegments_;
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
          (*target_int) * segments[i].k + segments[i].b - 10;
      if (result < 0)
      {
        result = 0;
      }
      if (result >= num_keys_) {
        result = num_keys_ - 1;
      }
      return result;
    }
  }
  return num_keys_ - 1;
}

double PLR_Model::guessPositionUsingBinarySearch(Slice target_key)
{
  std::vector<Segment> &segments = model_->lineSegments_;
#if USE_STRING_KEYS
  KEY_TYPE target_int_value = LdbKeyToInteger(target_key);
  KEY_TYPE *target_int = &target_int_value;
#else
  KEY_TYPE *target_int = (KEY_TYPE *)target_key.data_;
#endif

  int32_t left = 0, right = (int32_t)segments.size() - 1;
  while (left < right)
  {

    int32_t mid = (right + left + 1) / 2;
    if ((*target_int) < segments[mid].x)
      right = mid-1;
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

uint64_t PLR_Model::getNumberOfSegments() {
    std::cout<<"soemthing"<<std::endl;
    return (model_->lineSegments_).size() * sizeof(Segment);
}


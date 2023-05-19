#include "slice_iterator.h"
#include "math.h"

SliceIterator::SliceIterator() : model(nullptr), plr_segment_index(0) {}
uint64_t SliceIterator::getPLRLineSegmentIndex() { return plr_segment_index; }

void SliceIterator::setPLRLineSegmentIndex(uint64_t value) {
  plr_segment_index = value;
}
double SliceIterator::guessPosition(Slice target_key) {
  std::vector<Segment> &segments = model->lineSegments_;
  uint64_t target_int = LdbKeyToInteger(target_key.toString());
  for (uint64_t i = getPLRLineSegmentIndex(); i < (uint64_t)segments.size();
       i++) {
    if (segments[i].last > target_int) {
      setPLRLineSegmentIndex(i);
      double result = target_int * segments[i].k + segments[i].b;
      if (result < 0) {
        result = 0;
      }
      return floor(result);
    }
  }
  return target_int * segments[(uint64_t)segments.size() - 1].k +
         segments[(uint64_t)segments.size() - 1].b;
}

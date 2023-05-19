
#include "iterator.h"
#include "plr.h"
#include "slice.h"

class SliceIterator : public Iterator<Slice> {
public:
  SliceIterator();
  double guessPosition(Slice target_key) override;

protected:
  PLRModel *model;

private:
  uint64_t plr_segment_index;
  uint64_t getPLRLineSegmentIndex();
  void setPLRLineSegmentIndex(uint64_t value);
};

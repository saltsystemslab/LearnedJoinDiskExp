#include "binary_search.h"


double BinarySearch::guessPositionMonotone(Slice target_key) {
    Comparator<Slice> *c = new SliceComparator();
    return binary_search(0, a_->size()-1, target_key, c);
}

double BinarySearch::guessPositionUsingBinarySearch(Slice target_key) {
    Comparator<Slice> *c = new SliceComparator();
    return binary_search(0, a_->size()-1, target_key, c);
}


double BinarySearch::binary_search(int64_t start, int64_t end, Slice target_key, Comparator<Slice> *comparator)
    {
        while (start <= end)
        {
            int64_t mid = (start + end) / 2;
            if (comparator->compare(a_->at(mid), target_key) == 0)
            {
                return mid;
            }
            else if (comparator->compare(a_->at(mid), target_key) < 0)
            {
                start = mid+1;
            }
            else
            {
                end = mid-1;
            }
        }
        return -1;
    }


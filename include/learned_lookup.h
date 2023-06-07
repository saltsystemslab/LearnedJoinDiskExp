#ifndef LEARNEDLOOKUP_H
#define LEARNEDLOOKUP_H

#include "comparator.h"
#include "iterator.h"

class LearnedLookup
{
public:
    template <class T>
    static bool lookup(Iterator<T> *iterator,
                       uint64_t num_keys,
                       Comparator<T> *comparator,
                       T target_key)
    {
        comparator = new CountingComparator<T>(comparator);
        float approx_pos = iterator->guessPositionUsingBinarySearch(target_key);
        float start = approx_pos - PLR_ERROR_BOUND;
        float end = approx_pos + PLR_ERROR_BOUND;
        
        if (start < 0 || start >= num_keys)
        {
            start = 0;
        }
        if (end < 0 || end >= num_keys)
        {
            end = num_keys - 1;
        }
        while (comparator->compare(target_key, iterator->peek(start)) == -1)
        {
            start = start - PLR_ERROR_BOUND;
            if (start < 0)
            {
                start = 0;
                break;
            }
        }

        while (comparator->compare(target_key, iterator->peek(end)) == 1)
        {
            end = end + PLR_ERROR_BOUND;
            if (end >= num_keys)
            {
                end = num_keys - 1;
                break;
            }
        }

        bool status = binary_search(start, end, target_key, iterator, comparator);
        if (status == false)
        {
            //  KEY_TYPE sk = *((KEY_TYPE *)iterator->peek(start).data_);
            //  KEY_TYPE ek = *((KEY_TYPE *)iterator->peek(end).data_);
        }
        return status;
    }

private:
    template <class T>
    static bool binary_search(int64_t start, int64_t end, T target_key, Iterator<T> *iterator, Comparator<T> *comparator)
    {
        while (start <= end)
        {
            int64_t mid = (start + end) / 2;
            if (comparator->compare(iterator->peek(mid), target_key) == 0)
            {
                return true;
            }
            else if (comparator->compare(iterator->peek(mid), target_key) < 0)
            {
                start = mid + 1;
            }
            else
            {
                end = mid - 1;
            }
        }
        return false;
    }
};

#endif
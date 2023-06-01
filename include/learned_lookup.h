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
#if LEARNED_MERGE
        float approx_pos = iterator->guessPositionUsingBinarySearch(target_key);
        float start = approx_pos - PLR_ERROR_BOUND_FACTOR * PLR_ERROR_BOUND;
        float end = approx_pos + PLR_ERROR_BOUND_FACTOR * PLR_ERROR_BOUND;
        if (start < 0)
        {
            start = 0;
        }
        if (end >= num_keys)
        {
            end = num_keys - 1;
        }

        bool status = binary_search(start, end, target_key, iterator, comparator);
        if(status == false) {
            std::cout<<"start:"<<start<<" end:"<<end<<"approx_pos:"<<approx_pos<< " ";
            std::cout<<"element at start:"<<iterator->peek(start).toString()<<"element at end:"<<iterator->peek(end).toString()<<" ";
       }
#endif
    }

private:
    template <class T>
    static bool binary_search(uint64_t start, uint64_t end, T target_key, Iterator<T> *iterator, Comparator<T> *comparator)
    {
        while (start <= end)
        {
            uint64_t mid = (start + end) / 2;
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
#ifndef LOOKUP_H
#define LOOKUP_H

#include "comparator.h"
#include "iterator.h"

class StandardLookup
{
public:
    template <class T>
    static bool lookup(Iterator<T> *iterator,
                uint64_t num_keys,
                Comparator<T> *comparator,
                T target_key)
    {
        comparator = new CountingComparator<T>(comparator);
        return binary_search(0, num_keys - 1, target_key, iterator, comparator);
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
                start = mid+1;
            }
            else
            {
                end = mid-1;
            }
        }
        return false;
    }
};

#endif
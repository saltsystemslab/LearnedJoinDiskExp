#ifndef SLICE_BINARY_SEARCH_H
#define SLICE_BINARY_SEARCH_H

#include "plr.h"
#include "slice.h"
#include "model.h"
#include "iterator.h"
#include "comparator.h"
#include "slice_comparator.h"

class SliceBinarySearch : public Model<Slice>
{
public:
    SliceBinarySearch(std::vector<Slice> *a) : a_(a)
    {
    }
    uint64_t guessPositionMonotone(Slice target_key) override
    {
        Comparator<Slice> *c = new SliceComparator();
        return binary_search(0, a_->size() - 1, target_key, c);
    }
    uint64_t guessPosition(Slice target_key) override
    {
        Comparator<Slice> *c = new SliceComparator();
        return binary_search(0, a_->size() - 1, target_key, c);
    }
    SliceBinarySearch *get_model_for_subrange(uint64_t start, uint64_t end) override
    {
        return new SliceBinarySearch(a_); // incorrect
    }
    double getMaxError() override
    {
        0; // incorrect
    }
    uint64_t size_bytes() override
    {
        return 0;
    }

private:
    std::vector<Slice> *a_;
    double binary_search(int64_t start, int64_t end, Slice target_key, Comparator<Slice> *comparator)
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
                start = mid + 1;
            }
            else
            {
                end = mid - 1;
            }
        }
        return -1;
    }
};
class SliceBinarySearchBuilder : public ModelBuilder<Slice>
{
public:
    SliceBinarySearchBuilder(int n)
    {
        this->a_ = new std::vector<Slice>(n);
        this->n_ = n;
        this->cur_ = 0;
    }
    void add(const Slice &t) override
    {
        (*a_)[cur_++] = t;
    }
    SliceBinarySearch *finish() override
    {
        return new SliceBinarySearch(a_);
    }

private:
    std::vector<Slice> *a_;
    int n_;
    int cur_;
};

#endif
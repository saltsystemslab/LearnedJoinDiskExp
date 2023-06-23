#ifndef BINARY_SEARCH_H
#define BINARY_SEARCH_H

#include "plr.h"
#include "slice.h"
#include "model.h"
#include "iterator.h"
#include "comparator.h"
#include "slice_comparator.h"

template <class T>
class BinarySearch : public Model<T>
{
public:
    BinarySearch(std::vector<T> *a) : a_(a)
    {
    }
    uint64_t guessPositionMonotone(Slice target_key) override
    {
        Comparator<Slice> *c = new SliceComparator();
        return binary_search(0, a_->size() - 1, target_key, c);
    }
    uint64_t guessPosition(T target_key) override
    {
        Comparator<Slice> *c = new SliceComparator();
        return binary_search(0, a_->size() - 1, target_key, c);
    }
    Model<T> *get_model_for_subrange(const T &start, const T &end) override
    {
        return new BinarySearch(a_); // incorrect
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
    std::vector<T> *a_;
    double binary_search(int64_t start, int64_t end, T target_key, Comparator<Slice> *comparator)
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
template <class T>
class BinarySearchBuilder : public ModelBuilder<T>
{
public:
    BinarySearchBuilder(int n)
    {
        this->a_ = new std::vector<T>(n);
        this->n_ = n;
        this->cur_ = 0;
    }
    void add(const T &t) override
    {
        (*a_)[cur_++] = t;
    }
    BinarySearch *finish() override
    {
        return new BinarySearch(a_);
    }

private:
    std::vector<T> *a_;
    int n_;
    int cur_;
};

#endif
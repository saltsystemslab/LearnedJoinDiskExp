// #ifndef INT_BINARY_SEARCH_H
// #define INT_BINARY_SEARCH_H

// #include "plr.h"
// #include "slice.h"
// #include "model.h"
// #include "iterator.h"
// #include "comparator.h"
// #include "slice_comparator.h"

// template <class T>
// class IntBinarySearch : public Model<T>
// {
// public:
//     IntBinarySearch(std::vector<T> *a) : a_(a)
//     {
//     }
//     uint64_t guessPositionMonotone(T target_key) override
//     {
//         Comparator<Slice> *comparator = new SliceComparator();
//         comparator = new CountingComparator<T>(comparator);
//         return binary_search(0, a_->size() - 1, target_key, comparator);
//     }
//     uint64_t guessPosition(T target_key) override
//     {
//         Comparator<Slice> *comparator = new SliceComparator();
//         comparator = new CountingComparator<T>(comparator);
//         return binary_search(0, a_->size() - 1, target_key, comparator);
//     }
//     Model<T> *get_model_for_subrange(uint64_t start, uint64_t end) override
//     {
//         return new IntBinarySearch(a_); // incorrect
//     }
//     double getMaxError() override
//     {
//         0; // incorrect
//     }
//     uint64_t size_bytes() override
//     {
//         return 0;
//     }

// private:
//     std::vector<T> *a_;
//     double binary_search(int64_t start, int64_t end, T target_key, Comparator<T> *comparator)
//     {
//         while (start <= end)
//         {
//             int64_t mid = (start + end) / 2;
//             if (comparator->compare(a_->at(mid), target_key) == 0)
//             {
//                 return mid;
//             }
//             else if (comparator->compare(a_->at(mid), target_key) < 0)
//             {
//                 start = mid + 1;
//             }
//             else
//             {
//                 end = mid - 1;
//             }
//         }
//         return -1;
//     }
// };
// template <class T>
// class IntBinarySearchBuilder : public ModelBuilder<T>
// {
// public:
//     IntBinarySearchBuilder(int n)
//     {
//         this->a_ = new std::vector<T>(n);
//         this->n_ = n;
//         this->cur_ = 0;
//     }
//     void add(const T &t) override
//     {
//         (*a_)[cur_++] = t;
//     }
//     IntBinarySearch<T> *finish() override
//     {
//         return new IntBinarySearch<T>(a_);
//     }

// private:
//     std::vector<T> *a_;
//     int n_;
//     int cur_;
// };

// #endif
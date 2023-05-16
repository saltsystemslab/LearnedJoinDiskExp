#ifndef COMPARATOR_H
#define COMPARATOR_H

template <class T>
class Comparator{
    public:
        virtual int compare(T first_key, T sec_key) const = 0;
};
#endif
#ifndef ITERATOR_H
#define ITERATOR_H

template <class T>
class Iterator{
    public:
        virtual bool valid() const = 0;
        virtual void next() = 0;
        virtual T peek(uint64_t pos) const = 0;
        virtual void seek(T item) = 0;
        virtual void seekToFirst() = 0;
        virtual T key() const = 0;
        virtual double guessPosition(T target_key) {
            return -1;
        }

};
#endif
#include "include/merge_result_builder.h"
#include "include/comparator.h"
#include "include/iterator.h"

#include <iostream>
#include <cassert>

using namespace std;


class IntComparator: public Comparator<int> {
public:
    int compare(const int& a, const int& b) const override {
        if (a==b) return 0;
	if (a>b) return 1;
	return -1;
    }
};

class ArrayIntIterator: public Iterator<int> {
public:
    ArrayIntIterator(int *a, int n) {
        this->a = a;
	this->cur = 0;
	this->n = n;
    }
    ~ArrayIntIterator() {
        delete a;
    }
    bool valid() const override {
        return cur < n;
    }
    void next() override {
	assert(valid());
        cur++;
    }
    int peek(uint64_t pos) const override {
        return a[pos];
    }
    void seek(int item) override {
        for (int i=0; i<n; i++) {
	    if (a[i] > item) {
		cur = i;
		return;
	    }
	}
	cur = n;
    }
    void seekToFirst() override {
        cur = 0;
    }
    int key() const override {
        return a[cur];
    }
    double guessPosition(int target_key) {
        for (int i=0; i<n; i++) {
	    if (a[i] > target_key) {
		return cur;
	    }
	}
	return cur;
    }

private:
    int *a;
    int cur;
    int n;
};

class IntMergeResultBuilder: public MergeResultBuilder<int> {
public:
    IntMergeResultBuilder(int n) {
        this->a = new int[n];
	this->cur = 0;
	this->n = n;
    }
    void add(int t) override {
        a[cur++] = t;
    }
    ArrayIntIterator* finish() {
        return new ArrayIntIterator(a, n);
    }
private:
    int *a;
    int n;
    int cur;
};

int main() {
    IntMergeResultBuilder *builder = new IntMergeResultBuilder(10);
    for (int i=0; i<10; i++) {
        builder->add(i);
    }
    ArrayIntIterator *iterator = builder->finish();
    while (iterator->valid()) {
        std::cout<<iterator->key()<<std::endl;
	iterator->next();
    }

    Comparator<int> *c = new IntComparator();
    cout<<"Comparing 3, 4 "<<c->compare(3,4)<<endl;
    cout<<"Comparing 4, 3 "<<c->compare(4,3)<<endl;
    cout<<"Comparing 4, 4 "<<c->compare(4,4)<<endl;

}

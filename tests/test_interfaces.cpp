#include "merge_result_builder.h"
#include "comparator.h"
#include "iterator.h"
#include "merge.h"
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
    IntMergeResultBuilder *builder1 = new IntMergeResultBuilder(10);
    for (int i=0; i<10; i++) {
        builder1->add(2*i);
    }
    IntMergeResultBuilder *builder2 = new IntMergeResultBuilder(10);
    for (int i=0; i<10; i++) {
        builder2->add(2*i+1);
    }
    
    Iterator<int>** iterators = new Iterator<int>*[2];
    iterators[0] = builder1->finish();
    iterators[1] = builder2->finish();

    Comparator<int> *c = new IntComparator();

    IntMergeResultBuilder *resultBuilder = new IntMergeResultBuilder(20);
    standardMerge(iterators, 2, c, resultBuilder);

    Iterator<int> *result = resultBuilder->finish();
    int i = 0;
    while(result->valid()){
        assert(result->key() == i++);
        result->next();
    }
    assert(i==20);
}

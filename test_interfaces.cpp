#include "include/merge_result_builder.h"
#include <iostream>

using namespace std;

template <class T>
class PrintMergeResultBuilder: public MergeResultBuilder<T> {
public:
    void add(T t) override {
        cout<<t<<endl;
    }
};

int main() {
    MergeResultBuilder<int> *builder = new PrintMergeResultBuilder<int>();
    for (int i=0; i<10; i++) {
        builder->add(i);
    }
}

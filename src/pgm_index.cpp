#include "pgm_index.h"
#include <iostream>

double PGMIndex::guessPositionMonotone(Slice target_key){
    KEY_TYPE *k = (KEY_TYPE *) target_key.data_;
    
    std::cout<<"pos"<<a_->search(*k).lo<<std::endl;
    return a_->search(*k).lo;
}

double PGMIndex::guessPositionUsingBinarySearch(Slice target_key){
    KEY_TYPE *k = (KEY_TYPE *) target_key.data_;
    return a_->search(*k).lo;
}
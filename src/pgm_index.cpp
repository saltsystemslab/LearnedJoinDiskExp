#include "pgm_index.h"
#include <iostream>

double PGMIndex::guessPositionMonotone(Slice target_key){
    KEY_TYPE *k = (KEY_TYPE *) target_key.data_;
    
    return a_->search(*k).lo;
}

double PGMIndex::guessPositionUsingBinarySearch(Slice target_key){
    KEY_TYPE *k = (KEY_TYPE *) target_key.data_;
    return a_->search(*k).pos;
}

uint64_t PGMIndex::getNumberOfSegments() {
    return a_->size_in_bytes();
}
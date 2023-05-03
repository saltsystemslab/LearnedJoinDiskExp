#ifndef MERGE_H_
#define MERGE_H_

#include "plr.h"
#include <map>
#include <iostream>
using namespace std;

PLRModel getModel(vector<uint64_t> arr, uint64_t n);

float GuessPositionFromPLR(uint64_t target_key, uint64_t index, map<uint64_t, PLRModel> models);

vector<uint64_t> learnedMerge(vector<vector<uint64_t>> data);

vector<uint64_t> standardMerge(vector<vector<uint64_t>> data);

#endif

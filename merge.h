#ifndef MERGE_H_
#define MERGE_H_

#include "plr.h"
#include <map>
#include <iostream>
using namespace std;

PLRModel* getModel(vector<std::string> arr, uint64_t n);

float GuessPositionFromPLR(const std::string& target_key, PLRModel* model);

vector<std::string> learnedMerge(vector<vector<std::string>> data, PLRModel** models);

vector<std::string> standardMerge(vector<vector<std::string>> data);

#endif

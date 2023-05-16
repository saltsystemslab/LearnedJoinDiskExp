#include "merge.h"
#include<cmath>
#include <iostream>
#include <map>
#include <chrono>
#include <cassert>

using namespace std;

// uint64_t LdbKeyToInteger(const std::string& str) {
//     const char* data = str.data();
//     size_t size = str.size();
//     uint64_t num = 0;
//     bool leading_zeros = true;

//     for (int i = 0; i < size; ++i) {
//         int temp = data[i];
//         // TODO: Figure out where the extra bytes are coming from
//         if (temp < '0' || temp >'9') break;
//         if (leading_zeros && temp == '0') continue;
//         leading_zeros = false;
//         num = (num << 3) + (num << 1) + temp - 48;
//     }
//     return num;
// }
//generate plr model for a vector
PLRModel* getModel(vector<std::string> arr, uint64_t n)
{
	std::cout<<"getmodel"<<std::endl;
	uint64_t prev_key = 0;
	uint64_t index=0;
	PLRBuilder plrBuilder;
	for(uint64_t i=0;i<n;i++)
	{
		prev_key = plrBuilder.processKey(arr[i], index, prev_key);
		index+=1;
	}
	PLRModel* plrModel;
	plrModel = plrBuilder.finishTraining();
	return plrModel;
}

//get the position of target_key(second smallest element) in the vector with smallest head
// float GuessPositionFromPLR(uint64_t target_key, PLRModel* model){
//     std::vector<Segment>& segments = model->plrModelSegments;
//     size_t s = model->plrModelSegments.size();
//     uint64_t keyCount = model->keyCount;
//   // binary search between segments
//   uint64_t count=0;
//     uint64_t left = 0, right = (uint64_t)segments.size() - 1;
//     while (left != right - 1 && left < right) {
//         count++;
//         uint64_t mid = (right + left) / 2;
//         if (target_key < segments[mid].x)
//         right = mid;
//         else
//         left = mid;
//     }
//     double result = target_key * segments[left].k + segments[left].b;
//     if(result < 0){
//         result = 0;
//     }
//     return result;
// }

float GuessPositionFromPLR(const std::string& target_key, PLRModel* model) {
	std::vector<Segment>& segments = model->plrModelSegments;
	uint64_t target_int = LdbKeyToInteger(target_key);
	for(uint64_t i = model->getPLRLineSegmentIndex(); i < (uint64_t)segments.size(); i++){
		if(segments[i].last > target_int){
			model->setPLRLineSegmentIndex(i);
			double result = target_int * segments[i].k + segments[i].b;
			if(result < 0){
				result = 0;
			}
			return floor(result);
		}
	}  
}

vector<std::string> standardMerge(vector<vector<std::string>> data)
{
	vector<uint64_t> pos;
	for(uint64_t i=0;i<data.size();i++)
		pos.push_back(0);
	vector<std::string> result;
	uint64_t num_comp=0;
	while(true)
	{
		uint64_t s = -1;
		for(uint64_t i=0;i<data.size();i++)
		{
			if(pos[i] == data[i].size())
				continue;
			if(s == -1)
			{
				s = i;
				continue;
			}
			if(data[s][pos[s]] > data[i][pos[i]])
				s = i;
			num_comp++;
		}
		if(s == -1)
			break;
		result.push_back(data[s][pos[s]]);
		pos[s] = pos[s] + 1;
	}
	cout<<"Standard comparison count: "<<num_comp<<"\n";
	return result;
}

vector<std::string> learnedMerge(vector<vector<std::string>> data, PLRModel** models) {
	vector<std::string> merged_array;
	uint64_t cdf_error = 0;
	vector<uint64_t> pos;
	for(uint64_t i=0;i<data.size();i++)
		pos.push_back(0);
	uint64_t num_comp = 0;
	int smallest = -1;
	int second_smallest = -1;
	while(true)
	{
		// FIND SMALLEST AND SECOND SMALLEST
		if(smallest != -1 && second_smallest!=-1) 
		{
			int old_smallest = smallest;
			smallest = second_smallest;
			second_smallest = -1;
			for (int i = 0; i < data.size(); i++) {
				if(pos[i] == data[i].size()) {
					continue;
				}
				if (i == smallest) {
					continue;
				}
				if (second_smallest == -1) {
					second_smallest = i;
					continue;
				}
				if(data[second_smallest][pos[second_smallest]] > data[i][pos[i]]){
					second_smallest = i;
					num_comp++;
				}
				else {
					num_comp++;
				}
			}
		}
		else
		{
			for (uint64_t i = 0; i < data.size(); i++) {
				if(pos[i] == data[i].size()) {
					continue;
				}
				if (smallest == -1) {
					smallest = i;
					continue;
				}
				if(data[smallest][pos[smallest]] > data[i][pos[i]]){
					second_smallest = smallest;
					smallest = i;
					num_comp++;
				}
				else if(second_smallest == -1)
				{
					second_smallest = i;
					num_comp++;
				}
				else if(data[second_smallest][pos[second_smallest]] > data[i][pos[i]])
				{
					second_smallest = i;
					num_comp+=2;
				}
				else
					num_comp+=2;
			}
		}
		// FOUND SMALLEST AND SECOND SMALLEST

		if(smallest == -1) {
			break;
		}
		// LAST LIST, SO LOAD TILL END
		if(second_smallest == -1)
		{
			// LOAD TO END OF SMALLEST
			for (uint64_t i=pos[smallest];i<data[smallest].size();i++)
			{
				merged_array.push_back(data[smallest][pos[smallest]]);
				pos[smallest] += 1;
			}
			break;
		}
		const std::string& target_key = data[second_smallest][pos[second_smallest]];
		float approx_pos = GuessPositionFromPLR(target_key, models[smallest]);

		if(approx_pos >= data[smallest].size()) {
			approx_pos = data[smallest].size();;
		}
		if(approx_pos < pos[smallest]) {
			approx_pos = pos[smallest];
		}

		if (approx_pos == pos[smallest]) {
			merged_array.push_back(data[smallest][pos[smallest]]);
			pos[smallest]++;
			while (pos[smallest] < data[smallest].size() 
					&& data[smallest][pos[smallest]] <= data[second_smallest][pos[second_smallest]]) {
				merged_array.push_back(data[smallest][pos[smallest]]);
				pos[smallest]++;
				num_comp++;
			}
			if (pos[smallest] < data[smallest].size()) {
				num_comp++;
			}
			continue;
		}

		while (approx_pos > pos[smallest] && 
				data[smallest][approx_pos-1] > data[second_smallest][pos[second_smallest]]) {
			approx_pos--;
			cdf_error++;
		}
		if (approx_pos > pos[smallest]) {
			cdf_error++;
		}

		while(pos[smallest] < approx_pos){
			merged_array.push_back(data[smallest][pos[smallest]]);
			pos[smallest] += 1;
		}

		while(pos[smallest] < data[smallest].size() &&
				data[smallest][pos[smallest]] <= data[second_smallest][pos[second_smallest]]) {
			merged_array.push_back(data[smallest][pos[smallest]]);
			pos[smallest] += 1;
			num_comp++;
		}
		if (pos[smallest] < data[smallest].size()) {
			num_comp++;
		}
	}
	std::cout<<"Learned comparison count optimised: "<<num_comp<<"\n";
	std::cout<<"CDF error count optimised: "<<cdf_error<<"\n";
	return merged_array;
}

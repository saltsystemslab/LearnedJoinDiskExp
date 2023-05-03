#include "merge.h"
#include<cmath>
#include <iostream>
#include <map>
#include <chrono>

using namespace std;

//generate plr model for a vector
PLRModel getModel(vector<uint64_t> arr, uint64_t n)
{
    uint64_t prev_key = 0;
    uint64_t index=0;
    PLRBuilder plrBuilder;
    for(uint64_t i=0;i<n;i++)
    {
        prev_key = plrBuilder.processKey(to_string(arr[i]), index, prev_key);
        index+=1;
    }
    PLRModel plrModel;
    plrModel = plrBuilder.finishTraining();
    return plrModel;
}

//get the position of target_key(second smallest element) in the vector with smallest head
float GuessPositionFromPLR(uint64_t target_key, PLRModel model){
    std::vector<Segment>& segments = model.plrModelSegments;
    size_t s = model.plrModelSegments.size();
    uint64_t keyCount = model.keyCount;
  // binary search between segments
  uint64_t count=0;
    uint64_t left = 0, right = (uint64_t)segments.size() - 1;
    while (left != right - 1 && left < right) {
        count++;
        uint64_t mid = (right + left) / 2;
        if (target_key < segments[mid].x)
        right = mid;
        else
        left = mid;
    }
    double result = target_key * segments[left].k + segments[left].b;
    if(result < 0){
        result = 0;
    }
    return result;
}

vector<uint64_t> standardMerge(vector<vector<uint64_t>> data)
{
    vector<uint64_t> pos;
    for(uint64_t i=0;i<data.size();i++)
        pos.push_back(0);
    vector<uint64_t> result;
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

vector<uint64_t> learnedMerge(vector<vector<uint64_t>> data) {

    map<uint64_t, PLRModel> models;
    for(uint64_t i=0;i<data.size();i++)
    {
        models[i]=getModel(data[i],data[i].size());
    }

    vector<uint64_t> merged_array;
    uint64_t cdf_error = 0;
    vector<uint64_t> pos;
    for(uint64_t i=0;i<data.size();i++)
        pos.push_back(0);
    uint64_t num_comp = 0;
    uint64_t smallest = -1;
    while(true)
    {
        uint64_t second_smallest = -1;
        if(smallest!=-1)
        {
            for (uint64_t i = 0; i < data.size(); i++) {
            if(pos[i] == data[i].size())
                continue;
            if (second_smallest == -1 || second_smallest == smallest) {
                second_smallest = i;
                continue;
            }
            if(data[second_smallest][pos[second_smallest]] > data[i][pos[i]] && smallest != i){
                second_smallest = i;
                num_comp++;
            }
            else
                num_comp++;
        }
        }
        else
        {
            for (uint64_t i = 0; i < data.size(); i++) {
            if(pos[i] == data[i].size())
                continue;
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
                second_smallest = smallest;
                num_comp+=2;
            }
            else
                num_comp+=2;
            }
        }
        if(smallest == -1)
            break;
        if(second_smallest == -1 || second_smallest==smallest)
        {
            for (uint64_t i=pos[smallest];i<data[smallest].size();i++)
            {
                merged_array.push_back(data[smallest][pos[smallest]]);
                pos[smallest] += 1;
            }
        }
        else
        {
            uint64_t target_key = data[second_smallest][pos[second_smallest]];
            float approx_pos = GuessPositionFromPLR(target_key, models[smallest]);
            approx_pos=floor(approx_pos);

            if(approx_pos < 0)
                approx_pos = 0;
            if(approx_pos >= data[smallest].size())
                approx_pos = data[smallest].size()-1;

            //over-prediction
            while(approx_pos > 0 && data[smallest][approx_pos] > data[second_smallest][pos[second_smallest]]){
                cdf_error += 1;
                approx_pos -= 1;
            }
            //under-prediction
            while(approx_pos < data[smallest].size() && data[smallest][approx_pos] <= data[second_smallest][pos[second_smallest]]){
                cdf_error += 1;
                approx_pos += 1;
            }
            for(uint64_t i=pos[smallest];i<approx_pos;i++)
            {
                merged_array.push_back(data[smallest][pos[smallest]]);
                pos[smallest] += 1;
            }
        }
        smallest = second_smallest;
    }
    std::cout<<"Learned comparison count optimised: "<<num_comp<<"\n";
    std::cout<<"CDF error count optimised: "<<cdf_error<<"\n";
    return merged_array;
}

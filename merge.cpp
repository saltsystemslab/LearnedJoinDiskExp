#include "merge.h"
#include<cmath>
#include <iostream>
#include <map>
#include <chrono>

using namespace std;

PLRModel getModel(vector<int> arr, int n)
{
    uint64_t prev_key = 0;
    int index=0;
    PLRBuilder plrBuilder;
    for(int i=0;i<n;i++)
    {
        prev_key = plrBuilder.processKey(to_string(arr[i]), index, prev_key);
        index+=1;
    }
    PLRModel plrModel;
    plrModel = plrBuilder.finishTraining();
    return plrModel;
}

float GuessPositionFromPLR(int target_key, int index, PLRModel model){
    std::vector<Segment>& segments = model.plrModelSegments;
    size_t s = model.plrModelSegments.size();
    int keyCount = model.keyCount;
  // binary search between segments
  int count=0;
    uint32_t left = 0, right = (uint32_t)segments.size() - 1;
    while (left != right - 1 && left < right) {
        count++;
        uint32_t mid = (right + left) / 2;
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

vector<int> Merge(vector<vector<int>> data) {

    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    auto t1 = high_resolution_clock::now();

    map<int, PLRModel> models;
    for(int i=0;i<data.size();i++)
    {
        models[i]=getModel(data[i],data[i].size());
    }
    vector<int> merged_array;
    int cdf_error = 0;
    vector<int> pos;
    for(int i=0;i<data.size();i++)
        pos.push_back(0);
    int num_comp = 0;
    while(true)
    {
        int smallest = -1;
        int second_smallest = -1;
        for (int i = 0; i < data.size(); i++) {
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
        if(smallest == -1)
            break;
        if(second_smallest == -1)
        {
            for (int i=pos[smallest];i<data[smallest].size();i++) 
            {
                merged_array.push_back(data[smallest][pos[smallest]]);
                pos[smallest] += 1;
            }
        }
        else
        {   
            int target_key = data[second_smallest][pos[second_smallest]];
            float approx_pos = GuessPositionFromPLR(target_key, smallest, models[smallest]);
            approx_pos=floor(approx_pos);
            if(approx_pos < 0)
                approx_pos = 0;
            if(approx_pos >= data[smallest].size())
                approx_pos = data[smallest].size()-1;

            while(approx_pos > 0 && data[smallest][approx_pos] > data[second_smallest][pos[second_smallest]]){
                cdf_error += 1;
                approx_pos -= 1;
            }
            while(approx_pos < data[smallest].size() && data[smallest][approx_pos] <= data[second_smallest][pos[second_smallest]]){
                cdf_error += 1;
                approx_pos += 1;
            }
            for(int i=pos[smallest];i<approx_pos;i++)
            {
                merged_array.push_back(data[smallest][pos[smallest]]);
                pos[smallest] += 1;
            }
        }
        
    }
    auto t2 = high_resolution_clock::now();
    auto ms_int = duration_cast<milliseconds>(t2 - t1);
    duration<double, std::milli> ms_double = t2 - t1;

    std::cout<<"\nLearned merge: "<<ms_double.count()<<"ms\n";
    std::cout<<"Learned comparison count: "<<num_comp<<"\n";
    std::cout<<"CDF error count: "<<cdf_error<<"\n";
    return merged_array;
}

vector<int> Merge1(vector<vector<int>> data) {

    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;
    auto t1 = high_resolution_clock::now();

    map<int, PLRModel> models;
    for(int i=0;i<data.size();i++)
    {
        models[i]=getModel(data[i],data[i].size());
    }

    vector<int> merged_array;
    int cdf_error = 0;
    vector<int> pos;
    for(int i=0;i<data.size();i++)
        pos.push_back(0);
    int num_comp = 0;
    int smallest = -1;
    while(true)
    {
        int second_smallest = -1;
        if(smallest!=-1)
        {
            for (int i = 0; i < data.size(); i++) {
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
            for (int i = 0; i < data.size(); i++) {
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
            for (int i=pos[smallest];i<data[smallest].size();i++) 
            {
                merged_array.push_back(data[smallest][pos[smallest]]);
                pos[smallest] += 1;
            }
        }
        else
        {   
            int target_key = data[second_smallest][pos[second_smallest]];
            float approx_pos = GuessPositionFromPLR(target_key, smallest, models[smallest]);
            approx_pos=floor(approx_pos);
            if(approx_pos < 0)
                approx_pos = 0;
            if(approx_pos >= data[smallest].size())
                approx_pos = data[smallest].size()-1;

            while(approx_pos > 0 && data[smallest][approx_pos] > data[second_smallest][pos[second_smallest]]){
                cdf_error += 1;
                approx_pos -= 1;
            }
            while(approx_pos < data[smallest].size() && data[smallest][approx_pos] <= data[second_smallest][pos[second_smallest]]){
                cdf_error += 1;
                approx_pos += 1;
            }
            for(int i=pos[smallest];i<approx_pos;i++)
            {
                merged_array.push_back(data[smallest][pos[smallest]]);
                pos[smallest] += 1;
            }
        }
        smallest = second_smallest;
        
    }
    auto t2 = high_resolution_clock::now();
    auto ms_int = duration_cast<milliseconds>(t2 - t1);
    duration<double, std::milli> ms_double = t2 - t1;

    std::cout<<"\nLearned merge optimised: "<<ms_double.count()<<"ms\n";
    std::cout<<"Learned comparison count optimised: "<<num_comp<<"\n";
    std::cout<<"CDF error count optimised: "<<cdf_error<<"\n";
    return merged_array;
}
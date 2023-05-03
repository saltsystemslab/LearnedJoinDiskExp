#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <random>

#include "merge.h"

using std::chrono::high_resolution_clock;
using std::chrono::duration_cast;
using std::chrono::duration;
using std::chrono::milliseconds;

uint64_t random64() {
    std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dist;
    return dist(rng);
}

int main(int argc, char **argv)
{
    //BEST CASE : 1
    //AVERAGE CASE : 2
    //WORST CASE : 3

    cout<<"Start Test\n";
    int test_case = argv[1][0] - '0';

    vector<uint64_t> arr1;
    vector<uint64_t> arr2;
    vector<uint64_t> arr3;
    vector<uint64_t> arr4;

    //AVERAGE CASE
    if(test_case==2)
    {
        std::cout<<"AVERAGE CASE : \n";
        for (uint64_t x=0;x<1000000;x++)
        {
            arr1.push_back(random64());
        }
        for (uint64_t x=0;x<1000000;x++)
        {
            arr2.push_back(random64());
        }
        for (uint64_t x=0;x<1000000;x++)
        {
            arr3.push_back(random64());
        }
        for (uint64_t x=0;x<1000000;x++)
        {
            arr4.push_back(random64());
        }
        sort(arr1.begin(), arr1.end());
        sort(arr2.begin(), arr2.end());
        sort(arr3.begin(), arr3.end());
        sort(arr4.begin(), arr4.end());
    }

    //WORST CASE
    if(test_case==3)
    {
        std::cout<<"WORST CASE : \n";
        vector<uint64_t> sample;
        for(uint64_t x=0;x<4000000;x++)
        {
            sample.push_back(random64());
        }
        sort(sample.begin(), sample.end());
        for(uint64_t x=0;x<sample.size();x=x+4)
        {
            arr1.push_back(sample[x]);
            arr2.push_back(sample[x+1]);
            arr3.push_back(sample[x+2]);
            arr4.push_back(sample[x+3]);
        }
    }
    
    //BEST CASE
    if(test_case==1)
    {
        std::cout<<"BEST CASE : \n";
        uint64_t lower = 10000000;  // lower limit of the range
        uint64_t upper = 20000000;  // upper limit of the range
        for(uint64_t x=0;x<1000000;x++)
        {
            uint64_t num = (rand() % (upper - lower + 1)) + lower;
            arr1.push_back(num);
        }
        lower = 20000000;
        upper = 30000000;
        for(uint64_t x=0;x<1000000;x++)
        {
            uint64_t num = (rand() % (upper - lower + 1)) + lower;
            arr2.push_back(num);
        }
        lower = 30000000;
        upper = 40000000;
        for(uint64_t x=0;x<1000000;x++)
        {
            uint64_t num = (rand() % (upper - lower + 1)) + lower;
            arr3.push_back(num);
        }
        lower = 40000000;
        upper = 50000000;
        for(uint64_t x=0;x<1000000;x++)
        {
            uint64_t num = (rand() % (upper - lower + 1)) + lower;
            arr4.push_back(num);
        }
        sort(arr1.begin(), arr1.end());
        sort(arr2.begin(), arr2.end());
        sort(arr3.begin(), arr3.end());
        sort(arr4.begin(), arr4.end());
    }

    //Prepare the 2d vector
    vector<vector<uint64_t>> matrix;
    matrix.push_back(arr1);
    matrix.push_back(arr2);
    matrix.push_back(arr3);
    matrix.push_back(arr4);
    vector<uint64_t> result1;
    vector<uint64_t> result2;
    duration<double, std::milli> ms_double;

    auto t1 = high_resolution_clock::now();

    vector<uint64_t> a1;
    vector<uint64_t> a2;

    result1 = learnedMerge(matrix);

    auto t2 = high_resolution_clock::now();

    auto ms_int = duration_cast<milliseconds>(t2 - t1);
    ms_double = t2 - t1;

    //std::cout<<"Learned merge optimised: "<<ms_double.count()<<" ms\n";

    for(uint64_t i=0;i<matrix.size()*1000000-1;i++)
    {
        assert(result1[i]<=result1[i+1]);
    }
    
    t1 = high_resolution_clock::now();

    result2 = standardMerge(matrix);

    t2 = high_resolution_clock::now();

    ms_int = duration_cast<milliseconds>(t2 - t1);
    ms_double = t2 - t1;

    for(uint64_t i=0;i<matrix.size()*1000000;i++)
    {
        assert(result1[i]==result2[i]);
    }
    
    //cout<<"Standard merge: "<<ms_double.count()<<"ms\n";
    return 0;
}

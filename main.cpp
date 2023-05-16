#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <random>

#include "merge.h"

static const uint64_t UNIVERSE_SIZE = 2000000000000000000;
static uint64_t num_keys;
static int key_size = 19;

using std::chrono::high_resolution_clock;
using std::chrono::duration_cast;
using std::chrono::duration;
using std::chrono::milliseconds;

uint64_t random64() {
    std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<uint64_t> dist;
    return dist(rng);
}

uint64_t random_uint64() {
    uint64_t result = random();
    result = result << 32;
    result = result + random();
    return result;
}

string generate_key(uint64_t key_value) {
  string key = to_string(key_value);
  string result = string(key_size - key.length(), '0') + key;
  return std::move(result);
}

int main(int argc, char **argv)
{
    //BEST CASE : 1
    //AVERAGE CASE : 2
    //WORST CASE : 3
   // key_size = atoi(argv[2]);

    cout<<"Start Test\n";
    int test_case = argv[1][0] - '0';

    vector<uint64_t> arr1;
    vector<uint64_t> arr2;
    vector<uint64_t> arr3;
    vector<uint64_t> arr4;

    vector<std::string> keys_1;
    vector<std::string> keys_2;
    vector<std::string> keys_3;
    vector<std::string> keys_4;
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

    if(test_case == 4){
        
        for(int i=0; i< 2000000; i++){
             keys_1.push_back(generate_key(random_uint64() % UNIVERSE_SIZE));
         }
         for(int i=0; i< 2000000; i++){
             keys_2.push_back(generate_key(random_uint64() % UNIVERSE_SIZE));
         }
         

         sort(keys_1.begin(), keys_1.end());
         sort(keys_2.begin(), keys_2.end());
         
    }
    //Prepare the 2d vector
    std::cout<<"testcase 4"<<std::endl;
    vector<vector<std::string>> matrix;
    matrix.push_back(keys_1);
    matrix.push_back(keys_2);
   // matrix.push_back(arr1);
   // matrix.push_back(arr2);
   // matrix.push_back(arr3);
   // matrix.push_back(arr4);
    vector<std::string> result1;
    vector<std::string> result2;
    duration<double, std::milli> ms_double;
    duration<double, std::milli> ms_double_train;
    PLRModel** models = new PLRModel*[matrix.size()];
    auto train_1 = high_resolution_clock::now();
    for(uint64_t i=0;i<matrix.size();i++)
    {
        models[i]=getModel(matrix[i],matrix[i].size());
    }
    auto train_2 = high_resolution_clock::now();
    ms_double_train = train_2 - train_1;

    std::cout<<"model training time"<<ms_double_train.count()<<" ms\n";

    auto t1 = high_resolution_clock::now();

    result1 = learnedMerge(matrix, models);

    auto t2 = high_resolution_clock::now();

    auto ms_int = duration_cast<milliseconds>(t2 - t1);
    ms_double = t2 - t1;

    std::cout<<"Learned merge optimised: "<<ms_double.count()<<" ms\n";

    for(uint64_t i=0;i<result1.size()-1;i++)
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
    
    std::cout<<"Standard merge: "<<ms_double.count()<<"ms\n";
    return 0;
}

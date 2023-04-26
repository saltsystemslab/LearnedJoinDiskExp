#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <cassert>
#include <chrono>

#include "merge.h"

using namespace std;

vector<int> standard_merge(vector<vector<int>> arr)
{
    vector<int> pos;
    for(int i=0;i<arr.size();i++)
        pos.push_back(0);
    vector<int> result;
    int num_comp=0;
    while(true)
    {
        int s = -1;
        for(int i=0;i<arr.size();i++)
        {
            if(pos[i] == arr[i].size())
                continue;
            if(s == -1)
            {
                s = i;
                continue;
            }
            if(arr[s][pos[s]] > arr[i][pos[i]])
                s = i;
            num_comp++;
        }
        if(s == -1)
            break;
        result.push_back(arr[s][pos[s]]);
        pos[s] = pos[s] + 1;
    }
    cout<<"\nStandard comparison count: "<<num_comp<<"\n";
    return result;
}

int main()
{
    cout<<"Start Test\n";
    vector<int> arr1;
    vector<int> arr2;
    vector<int> arr3;
    vector<int> arr4;

    // //AVERAGE CASE
    // for (int x=0;x<100;x++)
    // {
    // arr1.push_back(rand()%50000);
    // }
    // for (int x=0;x<100;x++)
    // {
    // arr2.push_back(rand()%50000);
    // }
    // for (int x=0;x<100;x++)
    // {
    // arr3.push_back(rand()%50000);
    // }
    // for (int x=0;x<100;x++)
    // {
    // arr4.push_back(rand()%50000);
    // }
    // sort(arr1.begin(), arr1.end());
    // sort(arr2.begin(), arr2.end());
    // sort(arr3.begin(), arr3.end());
    // sort(arr4.begin(), arr4.end());

    //WORST CASE
    // vector<int> test;
    // for(int x=100;x<500;x++)
    // {
    //     int num = rand()%100000;
    //     test.push_back(num);
    // }
    // sort(test.begin(), test.end());
    // for(int x=0;x<test.size();x=x+4)
    // {
    //     arr1.push_back(test[x]);
    //     arr2.push_back(test[x+1]);
    //     arr3.push_back(test[x+2]);
    //     arr4.push_back(test[x+3]);
    // }
    
    //BEST CASE
    for(int x=100;x<=500;x=x+4)
    {
        int lower = 10000;  // lower limit of the range
        int upper = 20000;  // upper limit of the range
   
        //srand(time(0));  // seed the random number generator with the current time
   
        int num = (rand() % (upper - lower + 1)) + lower;
        arr1.push_back(num);
        lower = 20000;
        upper = 30000;
        //srand(time(0));
        num = (rand() % (upper - lower + 1)) + lower;
        arr2.push_back(num);
        lower = 30000;
        upper = 40000;
        //srand(time(0));
        num = (rand() % (upper - lower + 1)) + lower;
        arr3.push_back(num);
        lower = 40000;
        upper = 50000;
        //srand(time(0));
        num = (rand() % (upper - lower + 1)) + lower;
        arr4.push_back(num);
    }
     sort(arr1.begin(), arr1.end());
     sort(arr2.begin(), arr2.end());
     sort(arr3.begin(), arr3.end());
     sort(arr4.begin(), arr4.end());
    std::cout<<"\n ARRAY 1 :\n";
    for(int i=0;i<arr1.size();i++)
    {
        cout<<arr1[i]<<" ";
    }
    std::cout<<"\n";
     std::cout<<"\n ARRAY 2 :\n";
    for(int i=0;i<arr2.size();i++)
    {
        cout<<arr2[i]<<" ";
    }
    std::cout<<"\n";
     std::cout<<"\n ARRAY 3 :\n";
    for(int i=0;i<arr3.size();i++)
    {
        cout<<arr3[i]<<" ";
    }
    std::cout<<"\n";
     std::cout<<"\n ARRAY 4 :\n";
    for(int i=0;i<arr4.size();i++)
    {
        cout<<arr4[i]<<" ";
    }
    std::cout<<"\n";
    vector<vector<int>> matrix;
    matrix.push_back(arr1);
    matrix.push_back(arr2);
    matrix.push_back(arr3);
    matrix.push_back(arr4);
    vector<int> result;
    vector<int> result2;

    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

    // auto t1 = high_resolution_clock::now();

    result = Merge(matrix);
    result2 = Merge1(matrix);

    // auto t2 = high_resolution_clock::now();

    // auto ms_int = duration_cast<milliseconds>(t2 - t1);
    // duration<double, std::milli> ms_double = t2 - t1;

    // std::cout << "Learned merge: " <<ms_int.count() << "ms\n";
    // std::cout << "Learned merge: " <<ms_double.count() << "ms\n";

    for(int i=0;i<matrix.size()*100-1;i++)
    {
        assert(result[i]<=result[i+1]);
    }
    for(int i=0;i<matrix.size()*100-1;i++)
    {
        //cout<<result2[i]<<" "<<result2[i+1]<<"\n";
        assert(result2[i]<=result2[i+1]);
    }
     vector<int> result1;
    auto t11 = high_resolution_clock::now();

    result1 = standard_merge(matrix);

    auto t21 = high_resolution_clock::now();

    auto ms_int1 = duration_cast<milliseconds>(t21 - t11);
    duration<double, std::milli> ms_double1 = t21 - t11;

    cout<<"Standard merge: "<<ms_double1.count()<<"ms\n";
    for(int i=0;i<matrix.size()*100-1;i++)
    {
        assert(result1[i]<=result1[i+1]);
    }
    return 0;
}
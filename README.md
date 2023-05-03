# MergeLibrary
A general purpose library to merge two sorted lists using piecewise linear regression.

How to run ?

g++ -c plr.cpp

g++ -c merge.cpp

g++ -c main.cpp

g++ -o main main.o merge.o plr.o


./main <parameter> where parameter = 1/2/3 ( 1= best case, 2 = average case, 3 = worst case)

Best case : 
            
               arr1 = 1 2 3 
            
               arr2 = 4 5 6
            
               arr3 = 7 8 9 
            
Average case : 
            
               arr1 = 4 7 9
            
               arr2 = 1 2 3
            
               arr3 = 5 6 8
            
Worst case :
             
               arr1 = 1 4 7
            
               arr2 = 2 5 8
            
               arr3 = 3 6 9

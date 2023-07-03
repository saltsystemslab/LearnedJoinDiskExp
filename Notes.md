
# Prior Work



## Theory: Analysis of Learned Merging vs Standard Merging: Error Bounds, Training Time, Comparison Count and Runtime

* *TODO

# Experiments



## Microbenchmarks: Merging Sorted Lists and Joins



### Joins

Standard Sort Merge:
* Sort 2 lists.
* For each item in shorter list, walk through larger list. 
* Save where you stopped in the larger list.

Learned Sort Merge:
* Sort 2 lists and build index
* For each item in shorter list, lookup in larger list model
* Search around that area in the larger list.


##### 8 bytes keys, list1: 10M (2500 common keys), In Memory

Absolute (sec)

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|0.383|0.804|0.782|2.06|14.96|
|10|1.09|0.802|0.897|2.051|14.468|
|50|4.144|1.068|1.234|2.17|14.699|
|60|4.912|1.152|1.199|2.224|14.614|
|80|6.827|1.232|1.302|2.289|15.2|
|100|7.966|1.303|1.497|2.382|14.892|

Relative:

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|0|-109.92|-104.18|-437.86|-3,806.01|
|10|0|26.42|17.71|-88.17|-1,227.34|
|50|0|74.23|70.22|47.64|-254.71|
|60|0|76.55|75.59|54.72|-197.52|
|80|0|81.95|80.93|66.47|-122.65|
|100|0|83.64|81.21|70.10|-86.94|

##### 8 bytes keys, list1: 10M (2500 common keys), On Disk

Absolute numbers (sec)

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|0.677|1.121|1.078|3.541|24.769|
|10|1.628|1.098|1.257|3.341|24.659|
|50|6.121|1.7|1.834|3.864|25.204|
|60|7.184|2.359|1.971|4.453|25.117|
|80|9.604|2.042|2.224|4.502|25.218|
|100|11.527|2.279|2.444|4.665|25.505|

Relative (%)

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|0|-65.58|-59.23|-423.04|-3,558.64|
|10|0|32.56|22.79|-105.22|-1,414.68|
|50|0|72.23|70.04|36.87|-311.76|
|60|0|67.16|72.56|38.02|-249.62|
|80|0|78.74|76.84|53.12|-162.58|
|100|0|80.23|78.80|59.53|-121.26|

Notes:
* The learned merger is O(smaller_list * lookup_cost)
* The standard sort merger is O(smaller_list + larger_list)
* Using binary search as an alternative to learned index results in horrible performance (Didn't show those results here)







## Analyzing Lookup and Studying Model Choice

### PLR Size, Memory 

Absolute size of model (KB). 8 byte keys.

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|list_2_size|list_2_size(KB)|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|10000000|78125|28018|2325|28|0|
|100000000|781250|280321|23191|280|2|
|500000000|3906250|1401593|116047|1405|14|
|600000000|4687500|1682112|139313|1682|17|
|800000000|6250000|2242684|185714|2246|22|
|1000000000|7812500|2803064|232107|2802|28|

Relative size of model (%)

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|list_2_size|list_2_size(KB)|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|10000000|78125|35.86|8.30|1.20|0.00|
|100000000|781250|35.88|8.27|1.21|0.71|
|500000000|3906250|35.88|8.28|1.21|1.00|
|600000000|4687500|35.89|8.28|1.21|1.01|
|800000000|6250000|35.88|8.28|1.21|0.98|
|1000000000|7812500|35.88|8.28|1.21|1.00|

### PLR Size, Training Time 

Absolute Duration (sec). 8 byte duration.

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|list_2_size|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|10000000|0.691|0.706|0.441|0.695|
|100000000|5.527|4.845|4.471|4.728|
|500000000|27.222|24.221|22.298|22.042|
|600000000|32.555|29.071|26.998|26.666|
|800000000|44.067|38.567|36.294|35.197|
|1000000000|54.52|48.991|44.53|46.077|

* PLR Training should be O(n), where n is number of items of that list.
* 

### Lookup (PLR vs PGM vs Binary Search)


##  Applications

* LSM Tree compactions
* Sort Merge Joins
* Batch updates in Graph databases



# Prior Work

## Learned Indexes speed up lookups of sorted lists

### LSM Trees
* [Bourbon](https://www.usenix.org/conference/osdi20/presentation/dai)
* [Google BigTable](https://research.google/pubs/pub50034/)

1. These 2 papers have results showing using Learned Indexes speed up lookups. 
2. The claim is that learned indexes are smaller, can be stored in memory. This results in lesser  I/Os (compared to a B+ tree). 
3. SSTables are updated infrequently, and you can build during compaction process. 

### Genomics
* [Sapling](https://academic.oup.com/bioinformatics/article/37/6/744/5941464)
* [LISA](https://www.biorxiv.org/content/10.1101/2020.12.22.423964v2)

1. They improve on string search algorithms that use binary search over a sorted list of strings.
2. Binary search has poor cache performance as there are a lot of random accesses. Bad cache performance.
3. Learned Indexes can narrow down binary search window to a window of size magnitudes lesser than a binary search. 

### B+ Trees
* [Updatable Learned Indexes Meet Disk-Resident DBMS -- From Evaluations to Design Choices](https://arxiv.org/abs/2305.01237)
Not clear what the conclusion is. From the SIGMOD presentation, the conclusion seemed to be  B+ trees should be 'preferred'. 


<hr/>

## Learned Indexes speed up sorting and joins
* [SageDB](https://www.cidrdb.org/cidr2019/papers/p117-kraska-cidr19.pdf)
	* Vision paper that proposes an idea to build a database with machine learning components. 
	* Knowing the distribution of data should speed up many database operations. 
* [The Case for Learned Sorting](https://dl-acm-org.ezproxy.lib.utah.edu/doi/10.1145/3318464.3389752)
	* Use Learned Indexes to write a faster version of radix sort
	* Using learning indexes as a drop in replacement for an index does not work.
		* Learned indexes must be written to take advantage of optimizations (data locality for cache) that SOTA sorting algorithms use to be competitive.
		* Paper uses techniques used in efficient Radix sort implementations.
		* Variable length keys are still tricky.
* [The Case for Learned In-Memory Joins](https://arxiv.org/abs/2111.08824)
	* Use learned indexes to speed up INLJ, Sort Merge Join and Hash Joins
	* Again, authors observe that using learning indexes as a drop in replacement for an index does not work. 
	*  *<TODO: So what do they do to make learned in-memory joins competitive>*
	* *<TODO: Why don't they study joins for disk-resident joins? Where the index cannot be held in memory?>*

<hr/>


# Experiments

## Theory: Analysis of Learned Merging vs Standard Merging: Error Bounds, Training Time, Comparison Count and Runtime

* *TODO

## LSM Tree Compaction: 

Compaction in LSM Trees are CPU bound on Key Comparisons. 
Use Learned Indexes to reduce number of comparisons.

### Experiment Setup

YCSB Benchmark A
Machine: alps.cs.utah.edu
Greedy PLR, Error Bound: 10, 128 bit keys

#### Results

*<TODO: Check if these were the latest correct run numbers>*

##### Comparison Count

|   |   |   |
|---|---|---|
|YCSB Workload|Comp count Reduction<br/> (LOAD phase)|Comp count<br/>(RUN phase)|
|Workload a|19.12 %|20.11 %|
|Workload b|18.26 %|21.37 %|
|Workload c|17.76 %||
|Workload d|9.79 %||
|Workload e|18.02 %|19.55 %|
|Workload f|4.63 %|18.61 %|



##### Runtime (Load Phase and Runtime)

Load Phase

|   |   |   |   |
|---|---|---|---|
||Standard merge|Learned merge|% Runtime Increase|
|Workload a|151s|208s|38.00|
|Workload b|147s|204s|38.41|
|Workload c|145s|204s|40.21|
|Workload d|19s|22s|16.38|
|Workload e|148s|202s|36.40|
|Workload f|20s|21s|5.12|



Run phase

|   |   |   |   |
|---|---|---|---|
||Standard merge|Learned merge|% Runtime Increase|
|Workload a| 578s |800s |38.44|
|Workload b| 47s |56s |18.54|
|Workload e| 50s |58s |14.57|
|Workload f| 9s |10s |2.33|




#### Analysis and Open Questions

* We do reduce count of comparisons by ~19%. 
* But wall clock time is still greater ~15-40% !!!

Why is wall clock time greater? Is it
* Engineering issue? (Bad implementation?)
	* We do train PLR model on the fly - as the new file is being written as a result of compaction
	* We put PLR model on disk along with SSTable
* Possible overheads
		* Error correction of position returned by PLR
		* Key conversion from String to double
		* Larger buffer to hold a block of keys
* Should training time be included as part of queries?
		* Is training time an overhead we cannot overcome if using only for compaction? 
		* Do we need to use the model for queries too? 
* Compaction details?
	*  What are the size of the lists being merged in a compaction? 
	* <TODO: Insert Compaction Stats>

Before we spent time analyzing engineering implementation, there was a more important question we were failing to answer - What are the expected gains? Given sorted lists of certain sizes, how much better do we expect to do?

To better answer that question, we shifted our focus on building a microbenchmark test as a proving ground for our hypothesis - learned indexes can speed up merging lists.





## Microbenchmarks: Merging Sorted Lists and Joins

### Merging Sorted Lists

Machine: Patagonia

Overall Trends:
* As size skewness increases, performance of learned merger gets better
	* The threshold ratio of doing better goes up as key size increases .i.e, We need more skewness in data list size to do better. 
	* (Why?) Need to profile this to be sure. Could be Slice to integer conversion overhead?

* As PLR error increases, performance of learned merger does worse.
	* Lowering PLR error cost is a tradeoff of training time, memory and inference time. (This should be shown below)

##### 10M items, 8 bytes, In Memory, Single Thread

Absolute numbers (sec)

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|0.388|1.015|1.033|1.572|8.742|
|10|2.156|2.359|2.572|3.701|14.742|
|50|9.645|6.466|6.185|7.601|19.236|
|60|11.609|6.884|7.554|8.502|20.575|
|80|15.748|8.588|8.798|10.393|22.631|
|100|19.123|10.682|10.853|13.111|24.746|


Relative to standard (%) 

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|0|-161.59|-166.23|-305.15|-2153.09|
|10|0|-9.41|-19.29|-71.66|-583.76|
|50|0|32.96|35.87|21.19|-99.44|
|60|0|40.70|34.92|26.76|-77.23|
|80|0|45.46|44.13|34.00|-43.70|
|100|0|44.14|43.24|31.4|-29.40|


Only 8 bytes shown here, but we have results for 16 byte and 32 byte keys. 


##### 10M items, 8 bytes, On Disk, Single Thread

* Disk Buffering: 4096 bytes copied at a time from disk into memory. 
	* Current Buffer - stores 4096 byte chunk containing  current iterator
	* Peek Byffer - stores 4096 byte chunk containing last peek() item
* Standard Merger uses only 'Current Buffer'
* Learned Merger uses both 'Current Buffer' and 'Peek Buffer'


Absolute numbers (sec)

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|0.888|1.292|1.315|2.318|13.683|
|10|3.065|3.317|3.61|5.628|23.104|
|50|14.165|10.519|10.012|13.724|32.17|
|60|16.878|11.132|12.506|15.375|35.025|
|80|22.443|14.718|14.607|17.651|37.6|
|100|28.211|17.315|17.887|20.88|41.412|

Relative numbers(%)

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard %|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|0|-45.50|-48.09|-161.04|-1440.88|
|10|0|-8.22|-17.78|-83.62|-653.80|
|50|0|25.74|29.32|3.11|-127.11|
|60|0|34.04|25.90|8.91|-107.52|
|80|0|34.42|34.92|21.35|-67.54|
|100|0|38.62|36.60|25.99|-46.79|





##### 10M items, 8bytes, In Memory, 4 Threads

Absolute numbers (sec)

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|**NaN**|0.438|0.469|1.03|6.328|
|10|0.909|1.213|1.585|2.37|12.111|
|50|4.325|2.741|2.767|3.541|13.601|
|60|4.188|3.089|3.891|3.553|13.851|
|80|5.12|3.588|4.292|4.215|20.522|
|100|7.533|5.172|4.097|4.611|15.213|

Relative Improvement (%)

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1.00|NaN|NaN|NaN|NaN|NaN|
|10.00|.00|-33.44|-74.37|-160.73|-1232.34|
|50.00|.00|36.62|36.02|18.13|-214.47|
|60.00|.00|26.24|**7.09**|15.16|-230.73|
|80.00|.00|29.92|16.17|17.68|-300.82|
|100.00|.00|31.34|45.61|38.79|-101.95|

**The trends are similar, but I suspect there are bugs!**

##### 10M items, 8bytes, On Disk, 4 Threads


|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|0.243|0.56|0.602|1.571|10.888|
|10|1.342|1.874|1.893|2.978|19.083|
|50|5.773|5.131|5.07|6.064|23.691|
|60|6.903|6.309|6.394|6.981|21.332|
|80|8.986|8.738|8.648|8.05|32.31|
|100|10.235|10.401|9.461|9.831|23.38|

Relative numbers (%)

|   |   |   |   |   |   |
|---|---|---|---|---|---|
|ratio|standard|plr_error=2|plr_error=10|plr_error=100|plr_error=1000|
|1|0|-130.45|-147.74|-546.50|-4,380.66|
|10|0|-39.64|-41.06|-121.91|-1,321.98|
|50|0|11.12|12.18|-5.04|-310.38|
|60|0|8.60|7.37|-1.13|-209.03|
|80|0|2.76|3.76|10.42|-259.56|
|100|0|-1.62|7.56|3.95|-128.43|


**The gains are not clear here anymore! I suspect there are bugs or implementation can be more efficient. Need to profile this**

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


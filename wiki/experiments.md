## LSM Tree Compaction: 

Compaction in LSM Trees are CPU bound on Key Comparisons. 
Use Learned Indexes to reduce number of comparisons.

### Experiment Setup

YCSB Benchmark A
Machine: alps.cs.utah.edu
Greedy PLR, Error Bound: 10, 128 bit keys

#### Results

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
* As size skewness increases, performance of learned merger gets better. See [8 byte keys merge experiment](8_byte_merge.md)
	* The threshold ratio of doing better goes up as key size increases .i.e, We need more skewness in data list size to do better. 
	* (Why?) Need to profile this to root cause. See [32 byte keys merge experiment](32_byte_merge.md) - Even at ratios of 1:100 the learned merge does not beat standard merging.

* As PLR error increases, performance of learned merger does worse.
	* Lowering PLR error cost is a tradeoff of training time, memory and inference time.

### Joins

Standard Sort Merge:
* Sort 2 lists.
* For each item in shorter list, walk through larger list. 
* Save where you stopped in the larger list.

Learned Sort Merge:
* Sort 2 lists and build index
* For each item in shorter list, lookup in larger list model
* Search around that area in the larger list.
Standard Sort Merge:
* Sort 2 lists.
* For each item in shorter list, walk through larger list. 
* Save where you stopped in the larger list.

Learned Sort Merge:
* Sort 2 lists and build index
* For each item in shorter list, lookup in larger list model
* Search around that area in the larger list.


Notes:
* The learned merger is O(smaller_list * lookup_cost)
* The standard sort merger is O(smaller_list + larger_list)
* Using binary search as an alternative to learned index results in horrible performance (Didn't show those results here)

See experiment results for [8 byte key](8_byte_join.md), [16 byte key](16_byte_join.md), [32 byte key](32_byte_join.md)

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


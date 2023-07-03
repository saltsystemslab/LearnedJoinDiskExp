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


# Learned indexes for joins in external memory.

This repository contains the benchmark suite to evaluate learned indexes for joins in external memory. [Learned indexes](https://arxiv.org/abs/1712.01208) are indexes which use machine learning techniques to learn the distribution of data. They have been shown to be faster than B-Trees when operating in main-memory. We evaluate whether CDF-based indexes can speed up joins when the data is stored on external memory.


## Setup

First clone the Repo.

```bash
git clone git@github.com:saltsystemslab/LearnedJoinDiskExp
cd LearnedJoinDiskExp
git submodule update --init --recursive
```

### Setup datasets.

```bash
cd third_party/sosd/
sudo apt -y update
sudo apt -y install zstd python3-pip m4 cmake clang libboost-all-dev
pip3 install --user numpy scipy
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
./scripts/download.sh
```

### Experiments
In the root directory of the repo, to run the experiments for one dataset:

TODO: Finish commands

```
# Index study (comparing index size, build time and self-join performance)
./experiments/join_all/run.py --dataset=all --indexes=flatpgm256,sampledflatpgm256,btree256,radixspline256,btree256 --ratios=1 --exp_name=index_study --sosd_data_dir=../SOSD/data/

# Single Thread study (comparing inlj(btrees, sampledflatpgm), sort join, hash join) across various ratios
./experiments/join_all/run.py --dataset=all --indexes=sampledflatpgm256,btree256 --ratios=1,10,100,1000 --non_indexed_joins=sort_join,hash_join --exp_name=single_thread_join_study  --sosd_data_dir=../SOSD/data/

# Multithreaded Join (Single Dataset)
./experiments/join_all/run.py --dataset=fb --indexes=sampledflatpgm256,btree256 --ratios=1,10,100,1000 --non_indexed_joins=sort_join,hash_join  --threads=1,2,4,8,16 --exp_name=multithreaded_thread_join  --sosd_data_dir=../SOSD/data/

# Error Window Study
./experiments/join_all/run.py --dataset=all --indexes=sampledflatpgm256,sampledflatpgm1024,sampledflatpgm4096 -ratios=1,10,100,1000 --threads=1,2,4,8,16 --exp_name=error_window_study  --sosd_data_dir=../SOSD/data/

# String keys 
./experiments/join_all/run.py --dataset=string --indexes=btree256,sampledflatpgm256 -ratios=1,10,100,1000 --non_indexed_joins=sort_join --exp_name=string_keys  --sosd_data_dir=../SOSD/data/

# CGroups (Assumes this user can create/delete/edit the cgroup memory:learnedjoin/limit_mem)
./experiments/join_all/run.py --dataset=fb --indexes=sampledflatpgm256,btree256 -ratios=1,10,100,1000 --non_indexed_joins=sort_join --use_cgroups --cgroup_name=learnedjoin/limit_mem --cgroup_mem_limit=20M --exp_name=join_constrain_mem  --sosd_data_dir=../SOSD/data/

# Joins on unsorted data (exp_name=join_unsorted)
./experiments/join_unsorted/run.py --dataset=fb --phase=create_input 
./experiments/join_unsorted/run.py --dataset=fb --phase=run_test 

# Merge (Single threaded)
./experiments/merge/run.py --dataset=all --exp_name=single_thread_merge_study

# Merge (Merge Multithreaded )
./experiments/merge/run.py --dataset=fb --exp_name=multithread_merge_study --threads=1,2,4,8,16

```

### Generating reports.
Once the experiment here, we use `experiments/report.ipynb` as a base template to build exact queries. Examples of using this template are in results. 

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

./scripts/remove_duplicates_from_wiki.py 
./scripts/build_rmis.sh
# Make sure that config.h has set the right RMI_PATH.
```

### Experiments
In the root directory of the repo, to run the experiments for one dataset:

*WARNING: The experiments are setup to run with passwordless `sudo` to clear the OS cache between runs*

#### Single Thread study (comparing inlj(btrees, sampledflatpgm), sort join, hash join) across various ratios

To run on SSD

```bash
./experiments/join/run.py --dataset=all \
 --indexes=sampledflatpgm256,btree256 \
 --ratios=1,10,100,1000 \
 --non_indexed_joins=sort_join,hash_join \
 --exp_name=single_thread_join_study_ssd  \
 --sosd_data_dir=./third_party/SOSD/data/ \
 --test_dir=exp_runs
 ```

To run on HDD (assuming `/media/hdd/SOSD/data` and `/media/hdd` are on a hard disk).

 ```bash
./experiments/join/run.py --dataset=all \
 --indexes=sampledflatpgm256,btree256 \
 --ratios=1,10,100,1000 \
 --non_indexed_joins=sort_join,hash_join \
 --exp_name=single_thread_join_study_hdd  \
 --sosd_data_dir=./media/hdd/SOSD/data/ \
 --test_dir=/media/hdd/exp_runs
 ```

 #### Multithreaded thread study (comparing inlj (btrees, sampledflatpgm), sort join, hash join) across various ratios for different threads

```bash
./experiments/join/run.py --dataset=fb \
 --indexes=sampledflatpgm256,sampledflatpgm1024,sampledflatpgm4096,btree256 \
 --ratios=1,10,100,1000 \
 --threads=1,2,4,6,8 \
 --non_indexed_joins=sort_join,hash_join \
 --exp_name=single_thread_join_study_ssd  \
 --sosd_data_dir=./third_party/SOSD/data/ \
 --test_dir=exp_runs_multithread \
 ```

#### CGroups for join on unsorted data

Make sure the CgroupV2 exists and you can write to its process. In the case the cgroup is `/sys/fs/cgroup/join-cgroup`.

```bash
# Without memory limits
./experiments/join_unsorted/run.py --dataset=fb \
--phase=create_input \
--sosd_data_dir=./third_party/SOSD/data \
--test_dir=join_unsorted

./experiments/join_unsorted/run.py --dataset=fb \
--phase=run_test \
--sosd_data_dir=./third_party/SOSD/data \
--test_dir=join_unsorted_2G \
--use_cgroup \
--cgroup=/sys/fs/cgroup/join-cgroup

# Set memory limits
echo 2G > /sys/fs/cgroup/join-cgroup/memory.max 

./experiments/join_unsorted/run.py --dataset=fb \
--phase=create_input \
--sosd_data_dir=./third_party/SOSD/data \
--test_dir=join_unsorted_2G

./experiments/join_unsorted/run.py --dataset=fb \
--phase=run_test \
--sosd_data_dir=./third_party/SOSD/data \
--test_dir=join_unsorted_2G \
--use_cgroup \
--cgroup=/sys/fs/cgroup/join-cgroup

```

### Index study (comparing index size, build time and self-join performance)

```bash
./experiments/join/run.py --dataset=all \
--indexes=flatpgm256,sampledflatpgm256,btree256,radixspline256,btree256,rmi \
--ratios=1 \
--exp_name=index_study \
--sosd_data_dir=./third_party/SOSD/data/

./experiments/join/run_alex.py --dataset=all \
--ratios=1 \
--exp_name=index_study_alex \
--sosd_data_dir=./third_party/SOSD/data/

```

#### Generating Report CSV

Use the below python notebooks. Make sure to set the right test directory (`--exp_name` flag in the test runs) 

* `reports/join_singlethread.ipynb` (Figure 3a, 3b) 
* `reports/join_threads.ipynb` (Figure 5, 6) 
* `reports/join_unsorted_cgroup.ipynb` (Figure 4a)
* `reports/index_selfjoin.ipynb` (Figure 7)

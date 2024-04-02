# Learned indexes for joins in external memory.

This repository contains the benchmark suite to evaluate learned indexes for joins in external memory. 

TODO: Add more background.

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

```
# Index study
./scripts/join_all/run.py --dataset=fb --indexes=flatpgm256,sampledflatpgm256,btree256,radixspline256,btree256 --ratios=1
# Single Thread study
./scripts/join_all/run.py --dataset=fb --indexes=sampledflatpgm256,btree256 --ratios=1,10,100,1000
```



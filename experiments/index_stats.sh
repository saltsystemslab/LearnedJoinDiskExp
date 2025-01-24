#!/bin/bash
./experiments/join/run_alex.py \
--dataset=all \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1 \
--indexes=alex \
--test_dir=sponge_alex \
--clear_fs_cache 

./experiments/join/run.py  \
--dataset=all \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1 \
--indexes=btree256,sampledflatpgm256,flatpgm256,radixspline256,rmi \
--exp_name=index_stats \
--clear_fs_cache \
--test_dir=sponge \
--indexed_joins=lsj


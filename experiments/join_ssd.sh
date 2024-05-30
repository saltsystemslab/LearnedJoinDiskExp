#!/bin/bash
time ./experiments/join/run.py  \
--dataset=all \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1,10,100,1000 \
--indexes=btree256,sampledflatpgm256 \
--exp_name=join_ssd_all \
--clear_fs_cache \
--test_dir=sponge \
--indexed_joins=lsj,inlj \
--non_indexed_joins=sort_join,hash_join \
--io_device=sda

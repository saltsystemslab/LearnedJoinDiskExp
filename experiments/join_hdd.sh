#!/bin/bash
time ./experiments/join/run.py  \
--dataset=fb \
--sosd_data_dir=/media/datahdd/chesetti/SOSD/data \
--nouse_numactl \
--ratios=1,10,100,1000 \
--indexes=btree256,sampledflatpgm256 \
--exp_name=join_hdd \
--clear_fs_cache \
--test_dir=/media/datahdd/chesetti/sponge \
--indexed_joins=lsj,inlj \
--non_indexed_joins=sort_join,hash_join \
--io_device=sdb

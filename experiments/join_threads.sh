#!/bin/bash
time ./experiments/join/run.py  \
--dataset=fb \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1,10,100,1000 \
--threads=1 \
--indexes=sampledflatpgm256,sampledflatpgm1024,sampledflatpgm4096,btree256 \
--exp_name=join_threads \
--clear_fs_cache \
--test_dir=sponge \
--indexed_joins=lsj \
--io_device=sda

#!/bin/bash
time ./experiments/join/run.py  \
--dataset=fb \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1,10,100,1000 \
--threads=1,2,4,6,8 \
--indexes=sampledflatpgm256,sampledflatpgm1024,sampledflatpgm4096 \
--exp_name=join_threads_3 \
--clear_fs_cache \
--test_dir=sponge \
--indexed_joins=lsj \
--repeat=2 \
--io_device=sda

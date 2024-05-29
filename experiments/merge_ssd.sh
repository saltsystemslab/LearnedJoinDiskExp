#!/bin/bash
time ./experiments/merge/run.py  \
--dataset=fb \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--exp_name=merge_ssd \
--clear_fs_cache \
--test_dir=sponge \
--threads=1,2,4,6,8 \
--nouse_numactl \
--io_device=sda

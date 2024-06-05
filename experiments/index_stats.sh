#!/bin/bash

./experiments/join/run_alex.py \
--dataset=fb \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1 \
--test_dir=sponge_alex \
--clear_fs_cache \
--noclear_inputs \
--repeat=0

./experiments/join/run_alex.py \
--dataset=uniform_dense \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1 \
--test_dir=sponge_alex \
--clear_fs_cache \
--noclear_inputs \
--repeat=0

./experiments/join/run_alex.py \
--dataset=uniform_sparse \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1 \
--test_dir=sponge_alex \
--clear_fs_cache \
--noclear_inputs \
--repeat=0

./experiments/join/run_alex.py \
--dataset=lognormal \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1 \
--test_dir=sponge_alex \
--clear_fs_cache \
--noclear_inputs \
--repeat=0

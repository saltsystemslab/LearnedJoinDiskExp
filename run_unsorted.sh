#!/bin/bash

echo 512M > /sys/fs/cgroup/join-cgroup/memory.max
./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_512M_run2 
./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_512M_run2 --use_cgroup

#!/bin/bash

echo 512M > /sys/fs/cgroup/join-cgroup/memory.max
./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_512M
./experiments/join_unsorted/run.py --dataset=fb --phase=create_index --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_512M
./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup --use_cgroup --test_dir=sponge/join_unsorted_cgroup_512M


echo 256M > /sys/fs/cgroup/join-cgroup/memory.max
./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_256M
./experiments/join_unsorted/run.py --dataset=fb --phase=create_index --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_512M
./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup --use_cgroup --test_dir=sponge/join_unsorted_cgroup_256M


echo 128M > /sys/fs/cgroup/join-cgroup/memory.max
./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_128M
./experiments/join_unsorted/run.py --dataset=fb --phase=create_index --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_512M
./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup --use_cgroup --test_dir=sponge/join_unsorted_cgroup_128M

./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted
./experiments/join_unsorted/run.py --dataset=fb --phase=create_index --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_512M
./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted

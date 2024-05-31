#!/bin/bash
#./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_run0 
#./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_run0

#echo 512M > /sys/fs/cgroup/join-cgroup/memory.max
#./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_512M
#./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_512M --use_cgroup

echo 256M > /sys/fs/cgroup/join-cgroup/memory.max
./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_256M
./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_256M --use_cgroup

echo 128M > /sys/fs/cgroup/join-cgroup/memory.max
./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_128M
./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_128M --use_cgroup

echo 64M > /sys/fs/cgroup/join-cgroup/memory.max
./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_64M
./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_64M --use_cgroup

time ./experiments/join/run.py  \
--dataset=osm \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1,10,100,1000 \
--indexes=btree256,sampledflatpgm256 \
--exp_name=join_ssd_all \
--clear_fs_cache \
--test_dir=sponge \
--indexed_joins=lsj,inlj \
--non_indexed_joins=sort_join \
--io_device=sda

#!/bin/bash
time ./experiments/join/run.py  \
--dataset=books \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=1,10,100,1000 \
--indexes=btree256,sampledflatpgm256 \
--exp_name=join_ssd_all \
--clear_fs_cache \
--test_dir=sponge \
--indexed_joins=lsj,inlj \
--non_indexed_joins=sort_join \
--io_device=sda

#!/bin/bash
time ./experiments/join/run.py  \
--dataset=osm \
--sosd_data_dir=/media/datahdd/chesetti/SOSD/data \
--nouse_numactl \
--ratios=1,10,100,1000 \
--indexes=btree256,sampledflatpgm256 \
--exp_name=join_hdd_all \
--clear_fs_cache \
--test_dir=/media/datahdd/chesetti/sponge \
--indexed_joins=lsj,inlj \
--non_indexed_joins=sort_join \
--io_device=sdb

#!/bin/bash
time ./experiments/join/run.py  \
--dataset=books \
--sosd_data_dir=/media/datahdd/chesetti/SOSD/data \
--nouse_numactl \
--ratios=1,10,100,1000 \
--indexes=btree256,sampledflatpgm256 \
--exp_name=join_hdd_all \
--clear_fs_cache \
--test_dir=/media/datahdd/chesetti/sponge \
--indexed_joins=lsj,inlj \
--non_indexed_joins=sort_join \
--io_device=sdb


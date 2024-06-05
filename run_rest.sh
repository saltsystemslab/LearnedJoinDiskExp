#echo 4G > /sys/fs/cgroup/join-cgroup/memory.max
#./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_4G
#./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_4G --use_cgroup

#./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup
#./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup

#echo 2G > /sys/fs/cgroup/join-cgroup/memory.max
#./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_2G
#./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_cgroup_2G --use_cgroup

#time ./experiments/index_stats.sh





time ./experiments/join/run.py  \
--dataset=osm \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=10,100,1000 \
--indexes=btree256,sampledflatpgm256 \
--exp_name=join_ssd_all_2 \
--clear_fs_cache \
--test_dir=sponge \
--non_indexed_joins=hash_join \
--io_device=sda

#!/bin/bash
time ./experiments/join/run.py  \
--dataset=books \
--sosd_data_dir=/home/chesetti/sosd-data/ \
--nouse_numactl \
--ratios=10,100,1000 \
--indexes=btree256,sampledflatpgm256 \
--exp_name=join_ssd_all_2 \
--clear_fs_cache \
--test_dir=sponge \
--non_indexed_joins=hash_join \
--io_device=sda

#!/bin/bash
time ./experiments/join/run.py  \
--dataset=osm \
--sosd_data_dir=/media/datahdd/chesetti/SOSD/data \
--nouse_numactl \
--ratios=10,100,1000 \
--indexes=btree256,sampledflatpgm256 \
--exp_name=join_hdd_all_2 \
--clear_fs_cache \
--test_dir=/media/datahdd/chesetti/sponge \
--non_indexed_joins=hash_join \
--io_device=sdb

#!/bin/bash
time ./experiments/join/run.py  \
--dataset=books \
--sosd_data_dir=/media/datahdd/chesetti/SOSD/data \
--nouse_numactl \
--ratios=10,100,1000 \
--indexes=btree256,sampledflatpgm256 \
--exp_name=join_hdd_all_2 \
--clear_fs_cache \
--test_dir=/media/datahdd/chesetti/sponge \
--non_indexed_joins=hash_join \
--io_device=sdb


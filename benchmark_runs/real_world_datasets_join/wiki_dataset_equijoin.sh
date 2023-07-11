#!/bin/bash

make clean benchmark

echo "WIKI"
COMMON="./bin/benchmark --use_disk=1 --test_dir=wiki_equijoin --dataset=ar --data_file=./datasets/wiki_ts_200M_uint64 --num_datafile_keys=2000000 --split=1 --from_sosd=1 --key_bytes=8 --key_type=uint64"
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=binary_search
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=50

echo "FB"
COMMON="./bin/benchmark --use_disk=1 --test_dir=wiki_equijoin --dataset=ar --data_file=./datasets/fb_200M_uint64 --num_datafile_keys=2000000 --split=1 --from_sosd=1 --key_bytes=8 --key_type=uint64"
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=binary_search
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=50

echo "WIKI"
COMMON="./bin/benchmark --use_disk=1 --test_dir=wiki_equijoin --dataset=ar --data_file=./datasets/wiki_ts_200M_uint64 --num_datafile_keys=200000000 --split=1 --from_sosd=1 --key_bytes=8 --key_type=uint64"
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=binary_search
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=50

echo "FB"
COMMON="./bin/benchmark --use_disk=1 --test_dir=wiki_equijoin --dataset=ar --data_file=./datasets/fb_200M_uint64 --num_datafile_keys=200000000 --split=1 --from_sosd=1 --key_bytes=8 --key_type=uint64"
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=binary_search
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=50
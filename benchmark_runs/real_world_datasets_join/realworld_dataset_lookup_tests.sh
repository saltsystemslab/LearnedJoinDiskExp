#!/bin/bash

make clean benchmark

echo "WIKI"
BIN="./bin/benchmark --test_dir=rw_lookup --dataset=ar --data_file=./datasets/wiki_ts_200M_uint64 --num_datafile_keys=200000000 --num_of_lists=1 --split=1 --from_sosd=1 --num_lookup_queries=10000000 --key_bytes=8 --key_type=uint64"
${BIN}  --is_lookup_test=1 --use_disk=1 --index=pgm
${BIN}  --is_lookup_test=1 --use_disk=1 --index=plr --plr_error_bound=10
${BIN}  --is_lookup_test=1 --use_disk=1 --index=tree
${BIN}  --is_lookup_test=1 --use_disk=1 --index=binary_search

echo "FB"
BIN="./bin/benchmark --test_dir=rw_lookup --use_disk=1 --dataset=ar --data_file=./datasets/fb_200M_uint64 --num_datafile_keys=200000000 --num_of_lists=1 --split=1 --from_sosd=1 --num_lookup_queries=10000000 --key_bytes=8 --key_type=uint64"
${BIN}  --is_lookup_test=1 --use_disk=1 --index=pgm
${BIN}  --is_lookup_test=1 --use_disk=1 --index=plr --plr_error_bound=10
${BIN}  --is_lookup_test=1 --use_disk=1 --index=tree
${BIN}  --is_lookup_test=1 --use_disk=1 --index=binary_search

echo "AMAZON"
BIN="./bin/benchmark --test_dir=rw_lookup --use_disk=1 --dataset=ar --data_file=ar_keys_v1_00 --num_of_lists=1 --split=1 --num_lookup_queries=10000000 --key_bytes=16 --key_type=str"
${BIN}  --is_lookup_test=1 --use_disk=1 --index=pgm
${BIN}  --is_lookup_test=1 --use_disk=1 --index=tree
${BIN}  --is_lookup_test=1 --use_disk=1 --index=binary_search
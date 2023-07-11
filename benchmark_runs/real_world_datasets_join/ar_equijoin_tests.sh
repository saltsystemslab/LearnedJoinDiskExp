#!/bin/bash

make clean benchmark

# 0.1% EquiJoin
COMMON="./bin/benchmark --use_disk=1 --dataset=ar --data_file=ar_keys_v1_00 --split=0.001"
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=binary_search

# 1% EquiJoin
COMMON="./bin/benchmark --use_disk=1 --dataset=ar --data_file=ar_keys_v1_00 --split=0.01"
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=binary_search

# 10% EquiJoin
COMMON="./bin/benchmark --use_disk=1 --dataset=ar --data_file=ar_keys_v1_00 --split=0.1"
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=binary_search

# 100% EquiJoin
COMMON="./bin/benchmark --use_disk=1 --dataset=ar --data_file=ar_keys_v1_00 --split=1.0"
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=binary_search
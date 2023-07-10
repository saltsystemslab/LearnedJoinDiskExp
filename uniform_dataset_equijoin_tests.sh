#!/bin/bash

make clean benchmark

# 0.1% EquiJoin
COMMON="./bin/benchmark --use_disk=1 --num_common_keys=200000 --num_keys=0,199800000"
${COMMON} --merge_mode=no_op
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=64
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=100

# 1% EquiJoin
COMMON="./bin/benchmark --use_disk=1 --num_common_keys=2000000 --num_keys=0,198000000"
${COMMON} --merge_mode=no_op
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=64
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=100

# 10% EquiJoin
COMMON="./bin/benchmark --use_disk=1 --num_common_keys=20000000 --num_keys=0,18000000"
${COMMON} --merge_mode=no_op
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=64
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=100

# 100% EquiJoin
COMMON="./bin/benchmark --use_disk=1 --num_common_keys=200000000 --num_keys=0,0 "
${COMMON} --merge_mode=no_op
${COMMON} --merge_mode=learned_join --index=tree
${COMMON} --merge_mode=learned_join --index=pgm
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=64
${COMMON} --merge_mode=learned_join --index=plr --plr_error_bound=100
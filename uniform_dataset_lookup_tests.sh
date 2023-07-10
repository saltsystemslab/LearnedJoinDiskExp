#!/bin/bash

make clean benchmark

BIN="./bin/benchmark --test_dir=test_runs/plr_size --num_of_lists=1 --num_keys=200000000 --num_lookup_queries=10000000"

${BIN}  --is_binsearch_test=1 --use_disk=1
${BIN}  --is_lookup_test=1 --use_disk=1 --index=pgm
${BIN}  --is_lookup_test=1 --use_disk=1 --index=plr --plr_error_bound=10
${BIN}  --is_lookup_test=1 --use_disk=1 --index=plr --plr_error_bound=50
${BIN}  --is_lookup_test=1 --use_disk=1 --index=plr --plr_error_bound=500
${BIN}  --is_lookup_test=1 --use_disk=1 --index=tree
#!/bin/bash

make clean benchmark

BIN="./bin/benchmark --use_disk=1 --dataset=ar --data_file=ar_keys_v1_00 --num_of_lists=1"

${BIN}  --is_lookup_test=1 --use_disk=1 --index=pgm
${BIN}  --is_lookup_test=1 --use_disk=1 --index=tree
${BIN}  --is_lookup_test=1 --use_disk=1 --index=binary_search
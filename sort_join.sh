#!/bin/bash

make clean benchmark

COMMON="./bin/benchmark --use_disk=1 --dataset=ar --data_file=ar_keys_v1_00 --split=0.001"
${COMMON} --merge_mode=standard_join --index=dummy

COMMON="./bin/benchmark --use_disk=1 --dataset=ar --data_file=ar_keys_v1_00 --split=0.01"
${COMMON} --merge_mode=standard_join --index=dummy

COMMON="./bin/benchmark --use_disk=1 --dataset=ar --data_file=ar_keys_v1_00 --split=0.1"
${COMMON} --merge_mode=standard_join --index=dummy

COMMON="./bin/benchmark --use_disk=1 --dataset=ar --data_file=ar_keys_v1_00 --split=1.0"
${COMMON} --merge_mode=standard_join --index=dummy
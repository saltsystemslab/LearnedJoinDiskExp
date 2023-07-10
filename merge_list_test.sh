#!/bin/bash

COMMON="./bin/benchmark --use_disk=1 --num_keys=10000000,10000000 --key_bytes=8"
${COMMON} --merge_mode=no_op
${COMMON} --merge_mode=standard
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=50
${COMMON} --merge_mode=learned --index=pgm 
${COMMON} --merge_mode=learned --index=tree 

COMMON="./bin/benchmark --use_disk=1 --num_keys=10000000,100000000 --key_bytes=8"
${COMMON} --merge_mode=no_op
${COMMON} --merge_mode=standard
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=50
${COMMON} --merge_mode=learned --index=pgm 
${COMMON} --merge_mode=learned --index=tree 

COMMON="./bin/benchmark --use_disk=1 --num_keys=10000000,300000000 --key_bytes=8"
${COMMON} --merge_mode=no_op
${COMMON} --merge_mode=standard
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=50
${COMMON} --merge_mode=learned --index=pgm 
${COMMON} --merge_mode=learned --index=tree 

COMMON="./bin/benchmark --use_disk=1 --num_keys=10000000,500000000 --key_bytes=8"
${COMMON} --merge_mode=no_op
${COMMON} --merge_mode=standard
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=50
${COMMON} --merge_mode=learned --index=pgm 
${COMMON} --merge_mode=learned --index=tree 

COMMON="./bin/benchmark --use_disk=1 --num_keys=10000000,1000000000 --key_bytes=8"
${COMMON} --merge_mode=no_op
${COMMON} --merge_mode=standard
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=50
${COMMON} --merge_mode=learned --index=pgm 
${COMMON} --merge_mode=learned --index=tree 

COMMON="./bin/benchmark --use_disk=1 --num_keys=10000000,2000000000 --key_bytes=8"
${COMMON} --merge_mode=no_op
${COMMON} --merge_mode=standard
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=10
${COMMON} --merge_mode=learned --index=plr --plr_error_bound=50
${COMMON} --merge_mode=learned --index=pgm 
${COMMON} --merge_mode=learned --index=tree 
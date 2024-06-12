#!/bin/bash
./experiments/join_unsorted/run.py --dataset=fb --phase=create_input --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_verify
./experiments/join_unsorted/run.py --dataset=fb --phase=run_test --sosd_data_dir=/home/chesetti/sosd-data --test_dir=sponge/join_unsorted_verify 
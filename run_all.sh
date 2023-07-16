#!/bin/bash

timestamp=$(date +%s)

REPORT_DIR=benchmark_reports/

mkdir -p $REPORT_DIR
./benchmark_scripts/uniform_random_tests/run_plr_uniform_test.py uniform_random_plr_test | tee benchmark_reports/uniform_random_test_plr_report.txt
./benchmark_scripts/uniform_random_tests/uniform_dataset_equijoin_tests.sh | tee benchmark_reports/uniform_dataset_equijoin_report.txt
./benchmark_scripts/uniform_random_tests/uniform_dataset_lookup_tests.sh | tee benchmark_reports/uniform_dataset_lookup_report.txt

./benchmark_scripts/real_world_datasets/8byte_equijoin.sh | tee benchmark_reports/8byte_equijoin_report.txt
#!/bin/bash

./wiki_dataset_equijoin.sh | tee wiki_dataset_report.txt
./realworld_dataset_lookup_tests.sh | tee realworld_dataset_lookup_tests.txt
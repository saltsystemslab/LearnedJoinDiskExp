#!/bin/bash
./experiments/unsorted/run.py --phase=create_input --dataset=fb
./experiments/unsorted/run.py --phase=run_test --dataset=fb
./experiments/unsorted/run.py --phase=create_input --dataset=osm
./experiments/unsorted/run.py --phase=run_test --dataset=osm
./experiments/unsorted/run.py --phase=create_input --dataset=books
./experiments/unsorted/run.py --phase=run_test --dataset=books
./experiments/unsorted/run.py --phase=create_input --dataset=wiki
./experiments/unsorted/run.py --phase=run_test --dataset=wiki

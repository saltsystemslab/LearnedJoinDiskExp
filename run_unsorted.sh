#!/bin/bash
./experiments/unsorted/run.py --phase=create_input --dataset=fb
./experiments/unsorted/run.py --phase=run_test --dataset=fb

#!/bin/bash
find benchmark/dataset_ur_uint64/*.json | xargs -I {} ./benchmark/benchmark_runner.py --spec={} --repeat=1
find benchmark/dataset_fb/*.json | xargs -I {} ./benchmark/benchmark_runner.py --spec={} --repeat=1
find benchmark/dataset_wiki/*.json | xargs -I {} ./benchmark/benchmark_runner.py --spec={} --repeat=1
find benchmark/dataset_ar/*.json | xargs -I {} ./benchmark/benchmark_runner.py --spec={} --repeat=1
find benchmark/dataset_ur_16byte/*.json | xargs -I {} ./benchmark/benchmark_runner.py --spec={} --repeat=1

find benchmark/dataset_ur_uint64/*.json | xargs -I {} ./benchmark/benchmark_runner.py --spec={} --repeat=1 --regen_report

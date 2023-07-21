#!/bin/bash
find ./benchmark/unit_tests/ -name *.json | xargs -I{} ./benchmark/benchmark_runner.py --spec={} 
find ./benchmark/dataset_ur_uint64/ -name *.json | xargs -I{} ./benchmark/benchmark_runner.py --spec={} 
find ./benchmark/dataset_ar/ -name *.json | xargs -I{} ./benchmark/benchmark_runner.py --spec={} 

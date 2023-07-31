#!/bin/bash
find benchmark/dataset_ar/*.jsonnet | xargs -I {} jsonnet {} -o {}.json
find benchmark/dataset_ar/*.json | xargs -I {} ./benchmark/benchmark_runner.py --spec={} --repeat=1
find benchmark/dataset_ar/*.json | xargs -I {} rm {} 

find benchmark/dataset_osm/*.jsonnet | xargs -I {} jsonnet {} -o {}.json
find benchmark/dataset_osm/*.json | xargs -I {} ./benchmark/benchmark_runner.py --spec={} --repeat=1
find benchmark/dataset_osm/*.json | xargs -I {} rm {} 

find benchmark/dataset_ur_uint64/*.jsonnet | xargs -I {} jsonnet {} -o {}.json
find benchmark/dataset_ur_uint64/*.json | xargs -I {} ./benchmark/benchmark_runner.py --spec={} --repeat=1
find benchmark/dataset_ur_uint64/*.json | xargs -I {} rm {} 

find benchmark/dataset_ur_16byte/*.jsonnet | xargs -I {} jsonnet {} -o {}.json
find benchmark/dataset_ur_16byte/*.json | xargs -I {} ./benchmark/benchmark_runner.py --spec={} --repeat=1
find benchmark/dataset_ur_16byte/*.json | xargs -I {} rm {} 

#!/bin/bash

export ERROR_BOUND=1
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt

export ERROR_BOUND=2
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt

export ERROR_BOUND=4
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt

export ERROR_BOUND=8
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt

export ERROR_BOUND=16
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt

export ERROR_BOUND=32
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt

export ERROR_BOUND=64
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt

export ERROR_BOUND=128
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt

export ERROR_BOUND=256
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt

export ERROR_BOUND=512
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt

export ERROR_BOUND=1024
make clean benchmark
make print_options >>result.txt
./bin/benchmark --num_keys=2500000,2500000 >> result.txt
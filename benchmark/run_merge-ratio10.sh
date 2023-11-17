./scripts/benchmark.py --spec=benchmark/sosd_merge-ratio10.jsonnet --repeat=3 --threads=1 --sosd_source=/home/chesetti/sosd-data/fb_200M_uint64 --sosd_num_keys=200000000 --test_name=fb_merge-ratio10
./scripts/benchmark.py --spec=benchmark/sosd_merge-ratio10.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/fb_200M_uint64 --sosd_num_keys=200000000 --test_name=fb_merge-ratio10

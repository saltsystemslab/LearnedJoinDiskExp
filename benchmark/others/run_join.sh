./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=1 --sosd_source=/home/chesetti/sosd-data/fb_200M_uint64 --sosd_num_keys=200000000 --test_name=fb_join
./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=1 --sosd_source=/home/chesetti/sosd-data/wiki_ts_200M_uint64 --sosd_num_keys=200000000 --test_name=wiki_join
./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=1 --sosd_source=/home/chesetti/sosd-data/uniform_dense_200M_uint64 --sosd_num_keys=200000000 --test_name=uniform_dense_join
./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=1 --sosd_source=/home/chesetti/sosd-data/uniform_sparse_200M_uint64 --sosd_num_keys=200000000 --test_name=uniform_sparse_join
./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=1 --sosd_source=/home/chesetti/sosd-data/normal_200M_uint64 --sosd_num_keys=200000000 --test_name=normal_join
./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=1 --sosd_source=/home/chesetti/sosd-data/lognormal_200M_uint64 --sosd_num_keys=200000000 --test_name=lognormal_join
#./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=1 --sosd_source=/home/chesetti/sosd-data/books_800M_uint64 --sosd_num_keys=800000000 --test_name=books_join
#./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=1 --sosd_source=/home/chesetti/sosd-data/osm_cellids_800M_uint64 --sosd_num_keys=800000000 --test_name=osm_join

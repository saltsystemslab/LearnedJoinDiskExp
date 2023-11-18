./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/fb_200M_uint64 --sosd_num_keys=1000000 --test_name=fb_join_small
exit

./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/wiki_ts_200M_uint64 --sosd_num_keys=1000000 --test_name=wiki_join_small
/scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/uniform_dense_200M_uint64 --sosd_num_keys=1000000 --test_name=uniform_dense_join_small
./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/uniform_sparse_200M_uint64 --sosd_num_keys=1000000 --test_name=uniform_sparse_join_small
./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/normal_200M_uint64 --sosd_num_keys=1000000 --test_name=normal_join_small
./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/lognormal_200M_uint64 --sosd_num_keys=1000000 --test_name=lognormal_join_small
./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/books_800M_uint64 --sosd_num_keys=1000000 --test_name=books_join_small
./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/osm_cellids_800M_uint64 --sosd_num_keys=1000000 --test_name=osm_join_small

./scripts/benchmark.py --spec=benchmark/sosd_merge.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/fb_200M_uint64 --sosd_num_keys=1000000 --test_name=fb_merge_small
./scripts/benchmark.py --spec=benchmark/sosd_merge.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/wiki_ts_200M_uint64 --sosd_num_keys=1000000 --test_name=wiki_merge_small
./scripts/benchmark.py --spec=benchmark/sosd_merge.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/uniform_dense_200M_uint64 --sosd_num_keys=1000000 --test_name=uniform_dense_merge_small
./scripts/benchmark.py --spec=benchmark/sosd_merge.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/uniform_sparse_200M_uint64 --sosd_num_keys=1000000 --test_name=uniform_sparse_merge_small
./scripts/benchmark.py --spec=benchmark/sosd_merge.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/normal_200M_uint64 --sosd_num_keys=1000000 --test_name=normal_merge_small
./scripts/benchmark.py --spec=benchmark/sosd_merge.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/lognormal_200M_uint64 --sosd_num_keys=1000000 --test_name=lognormal_merge_small
./scripts/benchmark.py --spec=benchmark/sosd_merge.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/osm_cellids_800M_uint64 --sosd_num_keys=1000000 --test_name=osm_merge_small
./scripts/benchmark.py --spec=benchmark/sosd_merge.jsonnet --repeat=3 --threads=4 --sosd_source=/home/chesetti/sosd-data/books_800M_uint64 --sosd_num_keys=1000000 --test_name=books_merge_small
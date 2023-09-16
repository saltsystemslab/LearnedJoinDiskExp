find benchmark/dataset_ur_uint64/*.jsonnet | xargs -I {} jsonnet {} -o {}.json
find benchmark/dataset_ur_16byte/*.jsonnet | xargs -I {} jsonnet {} -o {}.json
find benchmark/dataset_fb/*.jsonnet | xargs -I {} jsonnet {} -o {}.json
find benchmark/dataset_wiki/*.jsonnet | xargs -I {} jsonnet {} -o {}.json
find benchmark/dataset_osm/*.jsonnet | xargs -I {} jsonnet {} -o {}.json


./benchmark/benchmark_runner.py --spec=benchmark/dataset_ur_uint64/join_disk_low.jsonnet.json --repeat=3
./benchmark/benchmark_runner.py --spec=benchmark/dataset_ur_uint64/join_disk_low_4threads.jsonnet.json --repeat=3
./benchmark/benchmark_runner.py --spec=benchmark/dataset_fb/join_disk_all.jsonnet.json --repeat=3
./benchmark/benchmark_runner.py --spec=benchmark/dataset_fb/join_disk_all_4.jsonnet.json --repeat=3
./benchmark/benchmark_runner.py --spec=benchmark/dataset_wiki/join_disk_all.jsonnet.json --repeat=3
./benchmark/benchmark_runner.py --spec=benchmark/dataset_wiki/join_disk_all_4threads.jsonnet.json --repeat=3
./benchmark/benchmark_runner.py --spec=benchmark/dataset_osm/join_disk_all.jsonnet.json --repeat=3
./benchmark/benchmark_runner.py --spec=benchmark/dataset_osm/join_disk_all_4.jsonnet.json --repeat=3

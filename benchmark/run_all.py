import subprocess

benchmark_script = "./scripts/benchmark.py"
join_args = [benchmark_script, "--spec=benchmark/sosd_join.jsonnet", "--repeat=2"]
merge_args = [benchmark_script, "--spec=benchmark/sosd_merge.jsonnet", "--repeat=2"]

datasets = {
    "lognormal": {
        "source": "/home/chesetti/sosd-data/lognormal_200M_uint64",
        "num_keys": 200000000
    },
    "uniform_sparse": {
        "source": "/home/chesetti/sosd-data/uniform_sparse_200M_uint64",
        "num_keys": 200000000
    },
    "uniform_dense": {
        "source": "/home/chesetti/sosd-data/uniform_dense_200M_uint64",
        "num_keys": 200000000
    },
    "normal": {
        "source": "/home/chesetti/sosd-data/normal_200M_uint64",
        "num_keys": 200000000
    },
    "osm": {
        "source": "/home/chesetti/sosd-data/osm_800M_uint64",
        "num_keys": 800000000
    },
    "wiki": {
        "source": "/home/chesetti/sosd-data/wiki_ts_200M_uint64",
        "num_keys": 200000000
    },
    "fb": {
        "source": "/home/chesetti/sosd-data/fb_200M_uint64",
        "num_keys": 200000000
    },
    "books": {
        "source": "/home/chesetti/sosd-data/books_800M_uint64",
        "num_keys": 800000000
    },
}

threads = [1, 4, 16, 32]
for name, dataset in datasets.items():
    for thread in threads:
        args = join_args
        args.append(f"--threads={thread}")
        args.append(f"--test_name=join_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        subprocess.run(args)

        args = merge_args
        args.append(f"--threads={thread}")
        args.append(f"--test_name=merge_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        subprocess.run(args)

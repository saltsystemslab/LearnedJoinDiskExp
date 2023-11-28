import subprocess

benchmark_script = "./scripts/benchmark.py"

# Datasets with 200M items. Single Thread. Run 3 times. 10,20,...100
datasets = {
    "fb": {
        "source": "/home/chesetti/sosd-data/fb_200M_uint64",
        "num_keys": 200000000
    },
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
    "wiki": {
        "source": "/home/chesetti/sosd-data/wiki_ts_200M_uint64",
        "num_keys": 200000000
    },
}

threads = [1]
for thread in threads:
    for name, dataset in datasets.items():
        args = join_args
        args.append(f"--threads={thread}")
        args.append(f"--repeat=2")
        args.append(f"--test_name=join_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        args.append(f'--clear_inputs=True')
        subprocess.run(args)

        args = merge_args
        args.append(f"--threads={thread}")
        args.append(f"--repeat=2")
        args.append(f"--test_name=merge_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        args.append(f'--clear_inputs=True')
        subprocess.run(args)

# Datasets with 800M items. Single Thread. Run 1 time. 10,20....100
datasets = {
    "osm": {
        "source": "/home/chesetti/sosd-data/osm_cellids_800M_uint64",
        "num_keys": 800000000
    },
    "books": {
        "source": "/home/chesetti/sosd-data/books_800M_uint64",
        "num_keys": 800000000
    },
}
threads = [1]
for thread in threads:
    for name, dataset in datasets.items():
        join_args = [benchmark_script, "--spec=benchmark/sosd_join.jsonnet"]
        args = join_args
        args.append(f"--threads={thread}")
        args.append(f"--repeat=0")
        args.append(f"--test_name=join_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        args.append(f'--clear_inputs=True')
        subprocess.run(args)

        merge_args = [benchmark_script, "--spec=benchmark/sosd_merge.jsonnet"]
        args = merge_args
        args.append(f"--threads={thread}")
        args.append(f"--repeat=0")
        args.append(f"--test_name=merge_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        args.append(f'--clear_inputs=True')
        subprocess.run(args)

# Datasets with 200M items. 32, 16, 4 Threads. Run 3 times. 10,50,100
datasets = {
    "fb": {
        "source": "/home/chesetti/sosd-data/fb_200M_uint64",
        "num_keys": 200000000
    },
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
    "wiki": {
        "source": "/home/chesetti/sosd-data/wiki_ts_200M_uint64",
        "num_keys": 200000000
    },
}

threads = [32, 16, 4]
for thread in threads:
    for name, dataset in datasets.items():
        join_args = [benchmark_script, "--spec=benchmark/sosd_join_small.jsonnet"]
        args = join_args
        args.append(f"--threads={thread}")
        args.append(f"--repeat=2")
        args.append(f"--test_name=join_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        args.append(f'--clear_inputs=True')
        subprocess.run(args)

        merge_args = [benchmark_script, "--spec=benchmark/sosd_merge_small.jsonnet"]
        args = merge_args
        args.append(f"--threads={thread}")
        args.append(f"--repeat=2")
        args.append(f"--test_name=merge_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        args.append(f'--clear_inputs=True')
        subprocess.run(args)

# Datasets with 800M items. 32, 16, 4 Threads. Run 1 times. 10,50,100
threads = [32, 16, 4]
datasets = {
    "osm": {
        "source": "/home/chesetti/sosd-data/osm_cellids_800M_uint64",
        "num_keys": 800000000
    },
    "books": {
        "source": "/home/chesetti/sosd-data/books_800M_uint64",
        "num_keys": 800000000
    },
}
for thread in threads:
    for name, dataset in datasets.items():
        join_args = [benchmark_script, "--spec=benchmark/sosd_join_small.jsonnet"]
        args = join_args
        args.append(f"--threads={thread}")
        args.append(f"--repeat=0")
        args.append(f"--test_name=join_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        args.append(f'--clear_inputs=True')
        subprocess.run(args)

        merge_args = [benchmark_script, "--spec=benchmark/sosd_merge_small.jsonnet"]
        args = merge_args
        args.append(f"--threads={thread}")
        args.append(f"--repeat=0")
        args.append(f"--test_name=merge_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        args.append(f'--clear_inputs=True')
        subprocess.run(args)

import subprocess

benchmark_script = "./scripts/benchmark.py"
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
    "osm": {
        "source": "/home/chesetti/sosd-data/osm_cellids_800M_uint64",
        "num_keys": 800000000
    },
    "books": {
        "source": "/home/chesetti/sosd-data/books_800M_uint64",
        "num_keys": 800000000
    },
}

for name, dataset in datasets.items():
        args = [benchmark_script, "--spec=experiments/index_size/sosd_index.jsonnet"]
        args.append(f"--threads=1")
        args.append(f"--repeat=0")
        args.append(f"--test_name=index_{name}")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        args.append(f'--clear_inputs=True')
        print(args)
        subprocess.run(args)
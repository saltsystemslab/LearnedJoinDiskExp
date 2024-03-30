#!/usr/bin/python3
import subprocess
import asyncio
from absl import app
from absl import flags
import os

FLAGS = flags.FLAGS

flags.DEFINE_integer("repeat", 2, "")
flags.DEFINE_string("dataset", "fb", "")
flags.DEFINE_bool("clear_inputs", True, "")
flags.DEFINE_bool("check_checksum", True, "")
flags.DEFINE_bool("skip_input", False, "")
flags.DEFINE_bool("use_numactl", True, "")
flags.DEFINE_string("sosd_data_dir", "./data", "")

def init_datasets():
 datasets = {
    "fb": {
        "source": os.path.join(FLAGS.sosd_data_dir, "fb_200M_uint64"),
        "num_keys": 200000000
    },
    "lognormal": {
        "source": os.path.join(FLAGS.sosd_data_dir, "lognormal_200M_uint64"),
        "num_keys": 200000000
    },
    "uniform_sparse": {
        "source": os.path.join(FLAGS.sosd_data_dir, "uniform_sparse_200M_uint64"),
        "num_keys": 200000000
    },
    "uniform_dense": {
        "source": os.path.join(FLAGS.sosd_data_dir, "uniform_dense_200M_uint64"),
        "num_keys": 200000000
    },
    "normal": {
        "source": os.path.join(FLAGS.sosd_data_dir, "normal_200M_uint64"),
        "num_keys": 200000000
    },
    "wiki": {
        "source": os.path.join(FLAGS.sosd_data_dir, "wiki_ts_200M_uint64"),
        "num_keys": 200000000
    },
    "osm": {
        "source": os.path.join(FLAGS.sosd_data_dir, "osm_cellids_800M_uint64"),
        "num_keys": 800000000
    },
    "books": {
        "source": os.path.join(FLAGS.sosd_data_dir, "books_800M_uint64"),
        "source": "/home/chesetti/sosd-data/books_800M_uint64",
        "num_keys": 800000000
    },
 }
 return datasets

def main(argv):
    datasets = init_datasets()
    benchmark_script = "./scripts/benchmark.py"
    test_config = "--spec=./experiments/join_multithread/join.jsonnet"
    for name, dataset in datasets.items():
        for thread in [1, 2, 4, 8, 16]:
            args = [benchmark_script, test_config]
            args.append(f"--threads={thread}")
            args.append(f"--repeat={FLAGS.repeat}")
            args.append(f"--test_dir=sponge/eps_study")
            args.append(f"--test_name={name}")
            args.append(f'--clear_inputs={FLAGS.clear_inputs}')
            args.append(f'--check_results={FLAGS.check_checksum}')
            args.append(f'--sosd_source={dataset["source"]}')
            args.append(f'--sosd_num_keys={dataset["num_keys"]}')
            args.append(f'--skip_input={FLAGS.skip_input}')
            args.append(f'--use_numactl={FLAGS.use_numactl}')
            print(args)
            subprocess.run(args)
        break

if __name__ == "__main__":
    app.run(main)

#!/usr/bin/python3
import subprocess
import asyncio
from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_integer("repeat", 2, "")
flags.DEFINE_integer("worker_threads", 5, "")
flags.DEFINE_string("dataset", "fb", "")
flags.DEFINE_bool("clear_inputs", True, "")
flags.DEFINE_bool("check_checksum", True, "")
flags.DEFINE_bool("skip_input", False, "")

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

def main(argv):
    benchmark_script = "./scripts/benchmark.py"
    test_config = "--spec=./experiments/join_all/join.jsonnet"
    for thread in [1]:
            args = [benchmark_script, test_config]
            args.append(f"--threads={thread}")
            args.append(f"--repeat={FLAGS.repeat}")
            args.append(f"--test_dir=sponge/join_all")
            args.append(f"--test_name={FLAGS.dataset}")
            args.append(f'--clear_inputs={FLAGS.clear_inputs}')
            args.append(f'--check_results={FLAGS.check_checksum}')
            args.append(f'--sosd_source={datasets[FLAGS.dataset]["source"]}')
            args.append(f'--sosd_num_keys={datasets[FLAGS.dataset]["num_keys"]}')
            args.append(f'--workers={FLAGS.worker_threads}')
            args.append(f'--skip_input={FLAGS.skip_input}')
            print(args)
            subprocess.run(args)

if __name__ == "__main__":
    app.run(main)

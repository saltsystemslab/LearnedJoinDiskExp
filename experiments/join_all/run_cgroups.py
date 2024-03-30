#!/usr/bin/python3
import subprocess
import asyncio
import os
from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_integer("repeat", 2, "")
flags.DEFINE_string("dataset", "fb", "")
flags.DEFINE_string("mem_limit", "1.2G", "")
flags.DEFINE_bool("clear_inputs", True, "")
flags.DEFINE_bool("only_input", False, "")
flags.DEFINE_bool("check_checksum", True, "")
flags.DEFINE_bool("skip_input", False, "")
flags.DEFINE_bool("use_numactl", False, "")
flags.DEFINE_bool("dry_run", False, "")
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
        "num_keys": 800000000
    },
 }
 return datasets

def main(argv):
    datasets = init_datasets()
    benchmark_script = "./scripts/benchmark.py"
    test_config = "--spec=./experiments/join_all/join.jsonnet"
    for thread in [1]:
            args = [benchmark_script, test_config]
            args.append(f"--threads={thread}")
            args.append(f"--repeat={FLAGS.repeat}")
            args.append(f"--test_dir=sponge/join_all_cgroups")
            args.append(f"--test_name={FLAGS.dataset}"+ f"_memlim={FLAGS.mem_limit}")
            args.append(f'--clear_inputs={FLAGS.clear_inputs}')
            args.append(f'--check_results={FLAGS.check_checksum}')
            args.append(f'--sosd_source={datasets[FLAGS.dataset]["source"]}')
            args.append(f'--sosd_num_keys={datasets[FLAGS.dataset]["num_keys"]}')
            args.append(f'--skip_input={FLAGS.skip_input}')
            args.append(f'--use_numactl={FLAGS.use_numactl}')
            args.append(f'--use_cgroups=True')
            args.append(f'--only_input={FLAGS.only_input}')
            args.append(f'--dry_run={FLAGS.dry_run}')
            args.append(f'--mem_limit={FLAGS.mem_limit}')
            print(args)
            subprocess.run(args)

if __name__ == "__main__":
    app.run(main)

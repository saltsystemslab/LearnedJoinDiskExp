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
flags.DEFINE_bool("use_numactl", False, "")
flags.DEFINE_string("sosd_data_dir", "./data", "")
flags.DEFINE_string("threads", "1","")
flags.DEFINE_string("exp_name", "mergetestrun","")
flags.DEFINE_bool("dry_run", False, "")

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
    test_config = "--spec=./experiments/merge/merge.jsonnet"
    threads = [int(t) for t in FLAGS.threads.split(",")]
    for thread in threads:
        for dataset in datasets:
            if FLAGS.dataset != "all" and FLAGS.dataset != dataset:  
                continue
            args = [benchmark_script, test_config]
            args.append(f"--threads={thread}")
            args.append(f"--repeat={FLAGS.repeat}")
            args.append(f"--test_dir=sponge/{FLAGS.exp_name}")
            args.append(f"--test_name={dataset}")
            args.append(f'--clear_inputs={FLAGS.clear_inputs}')
            args.append(f'--check_results={FLAGS.check_checksum}')
            args.append(f'--sosd_source={datasets[dataset]["source"]}')
            args.append(f'--sosd_num_keys={datasets[dataset]["num_keys"]}')
            args.append(f'--dry_run={FLAGS.dry_run}')
            args.append(f'--skip_input={FLAGS.skip_input}')
            print(args)
            subprocess.run(args)

if __name__ == "__main__":
    app.run(main)

#!/usr/bin/python3
import subprocess
import asyncio
import os
from absl import app
from absl import flags
import json

FLAGS = flags.FLAGS

flags.DEFINE_integer("repeat", 2, "")
flags.DEFINE_string("dataset", "fb", "")
flags.DEFINE_bool("clear_inputs", True, "")
flags.DEFINE_bool("check_checksum", True, "")
flags.DEFINE_bool("skip_input", False, "")
flags.DEFINE_bool("use_numactl", True, "")
flags.DEFINE_bool("dry_run", False, "")
flags.DEFINE_string("sosd_data_dir", "./third_party/sosd/data/", "")
flags.DEFINE_string("ratios", "1,10,100,100", "")
flags.DEFINE_string("indexes", "btree256,sampledflatpgm256","")
flags.DEFINE_string("non_indexed_joins", "sort_join","")
flags.DEFINE_string("exp_name", "testrun","")
flags.DEFINE_string("threads", "1","")
flags.DEFINE_bool("use_cgroups", False, "") 
flags.DEFINE_bool("clear_fs_cache", False, "") 
flags.DEFINE_string("cgroup", "/sys/fs/cgroup/join-cgroup","")
flags.DEFINE_bool("string_keys", False, "")
flags.DEFINE_string("io_device", "sdb", "")
flags.DEFINE_string("test_dir", "sponge", "")

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
    "string": {
        "source": "null",
        "num_keys": 100000000,
    }
 }
 return datasets

def main(argv):
    datasets = init_datasets()
    benchmark_script = "./scripts/benchmark.py"
    test_config = "--spec=./experiments/join/join.jsonnet"
    if FLAGS.string_keys:
        test_config = "--spec=./experiments/join/string_join.jsonnet"

    indexes = json.dumps(FLAGS.indexes.split(","))
    non_indexed_joins = json.dumps(FLAGS.non_indexed_joins.split(","))
    if not non_indexed_joins:
        non_indexed_joins = "[]"
    ratios = [int(r) for r in FLAGS.ratios.split(",")]
    threads = [int(t) for t in FLAGS.threads.split(",")]
    for thread in threads:
        for dataset in datasets:
            if FLAGS.dataset != "all" and FLAGS.dataset != dataset:  
                continue
            test_dir = os.path.join(FLAGS.test_dir, FLAGS.exp_name)
            args = [benchmark_script, test_config]
            args.append(f"--threads={thread}")
            args.append(f"--repeat={FLAGS.repeat}")
            args.append(f"--test_dir={test_dir}")
            args.append(f"--test_name={dataset}")
            args.append(f'--clear_inputs={FLAGS.clear_inputs}')
            args.append(f'--check_results={FLAGS.check_checksum}')
            args.append(f'--sosd_source={datasets[dataset]["source"]}')
            args.append(f'--sosd_num_keys={datasets[dataset]["num_keys"]}')
            args.append(f'--skip_input={FLAGS.skip_input}')
            args.append(f'--use_numactl={FLAGS.use_numactl}')
            args.append(f'--dry_run={FLAGS.dry_run}')
            args.append(f'--ratios={ratios}')
            args.append(f'--indexes={indexes}')
            args.append(f'--non_indexed_joins={non_indexed_joins}')
            args.append(f'--io_device={FLAGS.io_device}')
            if FLAGS.string_keys:
                args.append("--string_keys")
            if FLAGS.use_cgroups:
                args.append(f'--use_cgroups')
                args.append(f'--cgroup={FLAGS.cgroup}')
            if FLAGS.clear_fs_cache:
                args.append(f'--clear_fs_cache')
            print(args)
            subprocess.run(args)

if __name__ == "__main__":
    app.run(main)

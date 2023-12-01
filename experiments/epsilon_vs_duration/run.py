#!/usr/bin/python3
import subprocess
import asyncio
from absl import app
from absl import flags

exp_name = "eps_study"
benchmark_script = "./scripts/benchmark.py"
test_config = "--spec=./experiments/epsilon_vs_duration/join.jsonnet"

FLAGS = flags.FLAGS
flags.DEFINE_integer("repeat", 0, "")
flags.DEFINE_integer("worker_threads", 1, "")
flags.DEFINE_bool("clear_inputs", True, "")
flags.DEFINE_bool("check_checksum", True, "")

datasets = {
    "fb": {
        "source": "/home/chesetti/sosd-data/fb_200M_uint64",
        "num_keys": 200000000
    },
    "wiki": {
        "source": "/home/chesetti/sosd-data/fb_200M_uint64",
        "num_keys": 200000000
    },
}

def main(argv):
    for name, dataset in datasets.items():
        for thread in [1, 2, 4, 8, 16, 32]:
            args = [benchmark_script, test_config]
            args.append(f"--threads={thread}")
            args.append(f"--repeat={FLAGS.repeat}")
            args.append(f"--test_dir=sponge/{exp_name}")
            args.append(f"--test_name=join_{name}")
            args.append(f'--clear_inputs={FLAGS.clear_inputs}')
            args.append(f'--check_results={FLAGS.check_checksum}')
            args.append(f'--sosd_source={dataset["source"]}')
            args.append(f'--sosd_num_keys={dataset["num_keys"]}')
            args.append(f'--sosd_num_keys={dataset["num_keys"]}')
            args.append(f'--workers={FLAGS.worker_threads}')
            subprocess.run(args)

if __name__ == "__main__":
    app.run(main)

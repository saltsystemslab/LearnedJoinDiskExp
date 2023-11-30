#!/usr/bin/python3
import subprocess
from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_integer("repeat", 0, "")
flags.DEFINE_bool("clear_inputs", True, "")
flags.DEFINE_bool("check_checksum", True, "")

datasets = {
    "fb": {
        "source": "/home/chesetti/sosd-data/fb_200M_uint64",
        "num_keys": 5000000
    },
}

def main(argv):
    benchmark_script = "./scripts/benchmark.py"
    test_config = "--spec=./experiments/pgm_vs_flatpgm/join.jsonnet"
    for name, dataset in datasets.items():
        args = [benchmark_script, test_config]
        args.append(f"--threads=1")
        args.append(f"--repeat={FLAGS.repeat}")
        args.append(f"--test_name=join_{name}")
        args.append(f'--clear_inputs={FLAGS.clear_inputs}')
        args.append(f'--check_results={FLAGS.check_checksum}')
        args.append(f"--test_dir=sponge/pgm_vs_flat")
        args.append(f'--sosd_source={dataset["source"]}')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        subprocess.run(args)

if __name__ == "__main__":
    app.run(main)

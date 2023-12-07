#!/usr/bin/python3
import subprocess
from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_integer("repeat", 2, "")
flags.DEFINE_bool("clear_inputs", False, "")
flags.DEFINE_bool("check_checksum", False, "")

def main(argv):
    benchmark_script = "./scripts/benchmark.py"
    test_config = "--spec=./experiments/str_keys/string_join.jsonnet"
    for thread in [1, 2, 4, 8, 16]:
        args = [benchmark_script, test_config]
        args.append(f"--threads={thread}")
        args.append(f"--repeat={FLAGS.repeat}")
        args.append(f'--clear_inputs={FLAGS.clear_inputs}')
        args.append(f'--check_results={FLAGS.check_checksum}')
        args.append(f'--string_keys=True')
        args.append(f"--test_dir=sponge/string_keys")
        args.append(f"--test_name=string_keys")
        args.append(f'--sosd_num_keys=100000000')
        subprocess.run(args)

if __name__ == "__main__":
    app.run(main)

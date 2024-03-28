#!/usr/bin/python3
import json
import subprocess
import os
from absl import app
from absl import flags
import _jsonnet

FLAGS = flags.FLAGS

flags.DEFINE_string("phase", "create_input", "[create_input, run_test]")
flags.DEFINE_string("sosd_data_dir", "/home/chesetti/sosd-data", "")
flags.DEFINE_string("test_dir", "sponge/join_unsorted", "")
flags.DEFINE_string("dataset", "fb", "")

def init_datasets():
    return {
    "fb": {
        "source": os.path.join(FLAGS.sosd_data_dir, "fb_200M_uint64"),
        "num_keys": 200000000
        },
    }

def setup_experiment_directories():
    setup_script = "./scripts/benchmark.py"
    args = [setup_script, "--only_build", f"--test_dir={FLAGS.test_dir}", f"--test_name={FLAGS.dataset}"]
    print("Running setup script")
    print(args)
    subprocess.run(args)

def create_input_config(table_name):
    datasets = init_datasets()
    input_config_dir = os.path.join(FLAGS.test_dir, f"{FLAGS.dataset}_threads=1", "input_configs")
    input_dir = os.path.join(FLAGS.test_dir, f"{FLAGS.dataset}_threads=1", "inputs")
    input_config_template = './experiments/unsorted/create_input.jsonnet'
    table_config = json.loads(_jsonnet.evaluate_file(
        input_config_template, ext_vars = {
            "FRACTION": "0.1",
            "TEST_INPUT_FILE": datasets[FLAGS.dataset]["source"],
            "TEST_OUTPUT_FILE": os.path.join(input_dir, table_name)
        }
    ))
    config_path = os.path.join(input_config_dir, table_name)
    with open(config_path, "w") as outfile:
        config_json = json.dumps(table_config, indent=4)
        outfile.write(config_json)
    return config_path

def create_input(table_name, config_path):
    benchmark_runner = os.path.join(FLAGS.test_dir, "build", "benchmark_runner")
    process = subprocess.run([benchmark_runner, config_path], capture_output=True, text=True)
    print([benchmark_runner, config_path])
    input_result_dir = os.path.join(FLAGS.test_dir, f"{FLAGS.dataset}_threads=1", "input_results")
    if process.returncode == 0:
        result_json = json.loads(process.stdout)
        print(result_json)
        with open(os.path.join(input_result_dir, table_name), 'w') as outfile:
            outfile.write(json.dumps(result_json))
    else:
        print(process.stdout)
        print("Something wrong happened")


def main(argv):
    datasets = init_datasets()
    setup_experiment_directories()
    benchmark_runner = os.path.join(FLAGS.test_dir, "build", "benchmark_runner")

    if FLAGS.phase == "create_input":
        table1_config_path = create_input_config("table1")
        table2_config_path = create_input_config("table2")
        create_input("table1", table1_config_path)
        create_input("table2", table1_config_path)
        print(table1_config_path, table2_config_path)


if __name__ == "__main__":
    app.run(main)
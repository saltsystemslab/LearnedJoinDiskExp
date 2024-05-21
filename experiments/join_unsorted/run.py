#!/usr/bin/python3
import json
import subprocess
import os
import random
from absl import app
from absl import flags
import _jsonnet

FLAGS = flags.FLAGS

flags.DEFINE_string("phase", "create_input", "[create_input, run_test]")
flags.DEFINE_string("sosd_data_dir", "./third_party/sosd/data", "")
flags.DEFINE_string("test_dir", "sponge/join_unsorted", "")
flags.DEFINE_string("dataset", "fb", "")

def init_datasets():
    return {
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

def setup_experiment_directories():
    setup_script = "./scripts/benchmark.py"
    args = [setup_script, "--only_build", f"--test_dir={FLAGS.test_dir}", f"--test_name={FLAGS.dataset}"]
    print("Running setup script")
    print(args)
    subprocess.run(args)

def create_input_config(table_name, fraction):
    datasets = init_datasets()
    input_config_dir = os.path.join(FLAGS.test_dir, f"{FLAGS.dataset}_threads=1", "input_configs")
    input_dir = os.path.join(FLAGS.test_dir, f"{FLAGS.dataset}_threads=1", "inputs")
    input_config_template = './experiments/join_unsorted/create_input.jsonnet'
    table_config = json.loads(_jsonnet.evaluate_file(
        input_config_template, ext_vars = {
            "FRACTION": str(fraction),
            "TEST_INPUT_FILE": datasets[FLAGS.dataset]["source"],
            "TEST_OUTPUT_FILE": os.path.join(input_dir, table_name)
        }
    ))
    config_path = os.path.join(input_config_dir, table_name)
    with open(config_path, "w") as outfile:
        config_json = json.dumps(table_config, indent=4)
        outfile.write(config_json)
    return config_path

def get_table_path(table_name):
    input_dir = os.path.join(FLAGS.test_dir, f"{FLAGS.dataset}_threads=1", "inputs")
    return os.path.join(input_dir, table_name)


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

def create_output_configs(table1, table2, testcase):
    datasets = init_datasets()
    output_config_dir = os.path.join(FLAGS.test_dir, f"{FLAGS.dataset}_threads=1", "output_configs")
    output_config_template = './experiments/unsorted/join_table.jsonnet'
    output_dir = os.path.join(FLAGS.test_dir, f"{FLAGS.dataset}_threads=1", "outputs")
    output_configs = json.loads(_jsonnet.evaluate_file(
        output_config_template, ext_vars = {
            "TEST_NAME": testcase,
            "TEST_INNER_TABLE": table1,
            "TEST_OUTER_TABLE": table2,
            "TEST_OUTPUT_DIR": output_dir, 
            "TEST_REPEAT": "0",
        }
    ))
    config_paths = []
    for config in output_configs:
        output_config_path = os.path.join(output_config_dir, config["name"] + "_" + testcase)
        config_paths.append(output_config_path)
        with open(output_config_path, 'w') as outfile:
            config_json = json.dumps(config, indent=4)
            outfile.write(config_json)
    return config_paths

def run_config(config_path):
    benchmark_runner = os.path.join(FLAGS.test_dir, "build", "benchmark_runner")
    result_dir = os.path.join(FLAGS.test_dir, f"{FLAGS.dataset}_threads=1", "outputs", "results")
    process = subprocess.run([benchmark_runner, config_path], capture_output=True, text=True)
    print([benchmark_runner, config_path])
    config = os.path.basename(config_path)
    if process.returncode == 0:
        print(process.stdout)
        with open(os.path.join(result_dir, config), "w") as outfile:
            result_json = json.loads(process.stdout)
            #os.remove(result_json["spec"]["result_path"])
            try:
                #os.remove(result_json["spec"]["outer_index_file"])
                #os.remove(result_json["spec"]["inner_index_file"])
                pass
            except OSError as e:
                pass
            result_json = json.dumps(result_json, indent=4)
            outfile.write(result_json)
    else:
        print("SOmething went wrong!!!")

    

def main(argv):
    datasets = init_datasets()
    setup_experiment_directories()
    benchmark_runner = os.path.join(FLAGS.test_dir, "build", "benchmark_runner")
    if FLAGS.phase == "create_input":
        table1_config_path = create_input_config("table1", 1)
        table10_config_path = create_input_config("table10", 10)
        table100_config_path = create_input_config("table100", 100)
        table1000_config_path = create_input_config("table1000", 1000)
        create_input("table1", table1_config_path)
        create_input("table10", table10_config_path)
        create_input("table100", table100_config_path)
        create_input("table1000", table1000_config_path)
    
    if FLAGS.phase == "run_test":
        output_configs = []
        #output_configs.extend(create_output_configs(get_table_path("table1"), get_table_path("table1"), "1"))
        output_configs.extend(create_output_configs(get_table_path("table1"), get_table_path("table10"), "10"))
        output_configs.extend(create_output_configs(get_table_path("table1"), get_table_path("table100"), "100"))
        output_configs.extend(create_output_configs(get_table_path("table1"), get_table_path("table1000"), "1000"))
        output_configs.extend(create_output_configs(get_table_path("table1000"), get_table_path("table1000"), "sanity"))
        print(output_configs)
        random.shuffle(output_configs)
        for output_config in output_configs:
            run_config(output_config)


if __name__ == "__main__":
    app.run(main)

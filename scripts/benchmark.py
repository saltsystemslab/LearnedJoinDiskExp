#!/usr/bin/python3
import json
import os
import subprocess
import random
import pandas as pd
from absl import app
from absl import flags
import filecmp
import _jsonnet

FLAGS = flags.FLAGS

flags.DEFINE_string("spec", "", "JSON Test Spec")
flags.DEFINE_string("test_dir", "sponge", "JSON Test Spec")
flags.DEFINE_bool("skip_input", False, "Skip input creation")
flags.DEFINE_bool("check_results", True, "Verify that all the outputs are same.")
flags.DEFINE_bool("track_stats", False, "Use debug build and count microbenchmark stats.")
flags.DEFINE_bool("string_keys", False, "Use string keys.")
flags.DEFINE_bool("debug_build", False, "")
flags.DEFINE_integer("repeat", 3, "")
flags.DEFINE_integer("threads", 1, "")
flags.DEFINE_bool("regen_report", False, "")

flags.DEFINE_string("test_name", "unknown", "Test Case Name.")
flags.DEFINE_string("sosd_source", "unknown", "SOSD source dataset file")
flags.DEFINE_integer("sosd_num_keys", 100000000, "Num Keys in SOSD")

def main(argv):
    os.makedirs(FLAGS.test_dir, exist_ok=True)

    # Create benchmark_runner in sponge/build/
    # TODO(chesetti): Build to individual test directory.
    runner_bin = build_runner(os.path.join(FLAGS.test_dir, 'build')); 

    # Setup Experiment Directories
    exp = setup_experiment_directories()

    # Generate the experiment json config.
    # This config has definitions to generate input files, and test configurations.
    exp['config'] = json.loads(_jsonnet.evaluate_file(
        FLAGS.spec, ext_vars = {
            "TEST_OUTPUT_DIR": exp['output_dir'],
            "TEST_INPUT_DIR": exp['input_dir'],
            "TEST_REPEAT": str(FLAGS.repeat),
            "TEST_NUM_THREADS": str(FLAGS.threads),
            "TEST_DATASET_SIZE": str(FLAGS.sosd_num_keys),
            "TEST_DATASET_SOURCE": str(FLAGS.sosd_source)
        }
    ))

    # Benchmark Runner takes in a JSON config to either 
    #   a) generate an input SSTable
    #   b) run an experiment in a test (eg. join input 1 and input 2 using pgm)
    # Generate all the JSON configs.
    generate_configs(exp['config']['inputs'], exp['input_config_dir'])
    generate_configs(exp['config']['tests'], exp['output_config_dir'])
    run_configs(runner_bin, exp['input_config_dir'], exp['input_result_dir'], shuffle=False)
    run_configs(runner_bin, exp['output_config_dir'], exp['output_result_dir'], shuffle=True, total_repeat=FLAGS.repeat, delete_result_path=True)

def build_runner(build_dir, force_track_stats=False):
    track_stats='-DTRACK_STATS=ON' if (FLAGS.track_stats or force_track_stats) else '-DTRACK_STATS=OFF'
    string_keys = '-DSTRING_KEYS=ON' if (FLAGS.string_keys) else '-DSTRING_KEYS=OFF'
    debug = '-DCMAKE_BUILD_TYPE=debug' if (FLAGS.debug_build) else '-DCMAKE_BUILD_TYPE=release'
    subprocess.run(['cmake', '-B', build_dir, track_stats, string_keys, debug, '-S', '.'])
    subprocess.run(['cmake', '--build', build_dir, '-j'])
    return os.path.join(build_dir, 'benchmark_runner')

def get_specname():
    spec_path_with_extension = os.path.split(FLAGS.spec)[-1]
    return os.path.splitext(spec_path_with_extension)[0]

def setup_experiment_directories():
    experiment_dir = os.path.join(FLAGS.test_dir, FLAGS.test_name + "_"+str(FLAGS.threads))
    input_dir = os.path.join(experiment_dir, "inputs")
    output_dir = os.path.join(experiment_dir, "outputs")
    input_config_dir = os.path.join(input_dir, "configs")
    output_config_dir = os.path.join(output_dir, "configs")
    input_result_dir = os.path.join(input_dir, "results")
    output_result_dir = os.path.join(output_dir, "results")
    csv_dir = os.path.join(experiment_dir, "csv")
    os.makedirs(experiment_dir, exist_ok=True)
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(input_config_dir, exist_ok=True)
    os.makedirs(output_config_dir, exist_ok=True)
    os.makedirs(input_result_dir, exist_ok=True)
    os.makedirs(output_result_dir, exist_ok=True)
    os.makedirs(csv_dir, exist_ok=True)

    return {
        'base_dir': experiment_dir,
        'input_dir': input_dir,
        'output_dir': output_dir,
        'input_config_dir': input_config_dir,
        'output_config_dir': output_config_dir,
        'input_result_dir': input_result_dir,
        'output_result_dir': output_result_dir

    }

def generate_configs(config_list, output_dir):
    for config in config_list:
        config_path = os.path.join(output_dir, config["name"])
        with open(config_path, "w") as outfile:
            config_json = json.dumps(config, indent=4)
            outfile.write(config_json)

def run_configs(runner_bin, config_dir, result_dir, shuffle=True, total_repeat=1, delete_result_path=False):
        configs = os.listdir(config_dir)
        if shuffle:
            random.shuffle(configs)
        else:
            configs = sorted(configs)

        run_result_dir = os.path.join(result_dir, f'run')
        os.makedirs(run_result_dir, exist_ok=True)

        print(config_dir)
        print(configs)
        i = 0
        for config in configs:
            i = i + 1
            result_json = run([runner_bin, os.path.join(config_dir, config)], prefix="Running [%d/%d] %s" % (i, len(configs), config))
            if delete_result_path:
               os.remove(result_json['spec']['result_path'])
            test_result_file = result_json
            with open(os.path.join(run_result_dir, config), "w") as outfile:
                result_json = json.dumps(result_json, indent=4)
                outfile.write(result_json)


def run(command, force_dry_run=False, prefix=''):
    if FLAGS.regen_report:
        return
    command = ['numactl', '-N', '1', '-m', '1'] + command
    command_str = " ".join(command)
    result = {"command": command_str}
    print(prefix, ' '.join(command))
    process= subprocess.run(command, capture_output=True, text=True)
    if process.returncode == 0:
        result_json = json.loads(process.stdout)
        result.update(result_json)
    else:
        result['status'] = 'NZEC'
    return result

def remove_result(result_path):
    try:
        os.remove(result_path)
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    app.run(main)

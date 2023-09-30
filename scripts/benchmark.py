#!/usr/bin/python3
import json
import os
import subprocess
import random
import pandas as pd
from absl import app
from absl import flags
import filecmp
from matplotlib import pyplot as plt
import tikzplotlib
import _jsonnet

FLAGS = flags.FLAGS

flags.DEFINE_string("spec", "", "JSON Test Spec")
flags.DEFINE_string("run_dir", "sponge", "JSON Test Spec")
flags.DEFINE_bool("skip_input", False, "")
flags.DEFINE_bool("check_results", True, "")
flags.DEFINE_bool("track_stats", False, "")
flags.DEFINE_bool("string_keys", False, "")
flags.DEFINE_bool("debug_build", False, "")

runner_bin = './bench/benchmark_runner'
def build_runner(force_track_stats=False):
    track_stats='-DTRACK_STATS=ON' if (FLAGS.track_stats or force_track_stats) else '-DTRACK_STATS=OFF'
    string_keys = '-DSTRING_KEYS=ON' if (FLAGS.string_keys) else '-DSTRING_KEYS=OFF'
    debug = '-DCMAKE_BUILD_TYPE=debug' if (FLAGS.debug_build) else '-DCMAKE_BUILD_TYPE=release'
    subprocess.run(['cmake', '-B', 'bench', track_stats, string_keys, debug, '-S', '.'])
    subprocess.run(['cmake', '--build', 'bench', '-j'])

#https://github.com/nschloe/tikzplotlib/issues/557#issuecomment-1401501721
def tikzplotlib_fix_ncols(obj):
    """
    workaround for matplotlib 3.6 renamed legend's _ncol to _ncols, which breaks tikzplotlib
    """
    if hasattr(obj, "_ncols"):
        obj._ncol = obj._ncols
    for child in obj.get_children():
        tikzplotlib_fix_ncols(child)


def extract_table(results, metric_fields):
    results_table = []
    for result_json in results:
        metric_dict = result_json
        for field in metric_fields[:-1]:
            if metric_dict and field in metric_dict:
                metric_dict = metric_dict[field]
        metric_val = None
        if metric_dict and metric_fields[-1] in metric_dict:
            metric_val = metric_dict[metric_fields[-1]]
        if ('status' in result_json and result_json['status'] == 'NZEC'):
            print("WARNING: COMMAND FAILED, check %s" % result_json['command'])
            continue
        results_table.append(
            (result_json['spec']['label_x'],
            result_json['spec']['name'],
            metric_val))
    return results_table

def run(command, force_dry_run=False, prefix=''):
    command = ['numactl', '-N', '1', '-m', '1'] + command
    command_str = " ".join(command)
    result = {"command": command_str}
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

def main(argv):
    bench_dir = os.path.join(FLAGS.run_dir, os.path.splitext(FLAGS.spec)[0])
    config_dir = os.path.join(bench_dir, "configs")
    input_dir = os.path.join(bench_dir, "outputs")
    output_dir = os.path.join(bench_dir, "inputs")
    os.makedirs(config_dir, exist_ok=True)
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    

    json_str = _jsonnet.evaluate_file(
        FLAGS.spec, ext_vars = {
            "TEST_OUTPUT_DIR": input_dir,
            "TEST_INPUT_DIR": output_dir,
        }
    )

    build_runner();
    json_obj = json.loads(json_str)
    for input_config in json_obj["inputs"]:
        run_config = {}
        json_dict = json.dumps(input_config, indent=4)
        run_config['spec_path'] = os.path.join(config_dir, input_config["name"] + "_input.json")
        with open(run_config['spec_path'], "w") as outfile:
            outfile.write(json_dict)
        result_json = run([runner_bin, run_config['spec_path']], prefix="Running %s" % run_config['spec_path'])
        print(result_json)

    for test_config in json_obj["tests"]:
        run_config = {}
        json_dict = json.dumps(test_config, indent=4)
        run_config['spec_path'] = os.path.join(config_dir, test_config["name"] + "_test.json")
        with open(run_config['spec_path'], "w") as outfile:
            outfile.write(json_dict)
        result_json = run([runner_bin, run_config['spec_path']], prefix="Running %s" % run_config['spec_path'])
        print(result_json)
        

if __name__ == "__main__":
    app.run(main)

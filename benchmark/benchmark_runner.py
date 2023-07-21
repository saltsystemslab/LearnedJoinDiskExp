#!/usr/bin/python3
import json
import os
import subprocess
import random
import pandas as pd
from absl import app
from absl import flags
import filecmp

FLAGS = flags.FLAGS

flags.DEFINE_string("run_dir", "test_runs", "JSON Test Spec")
flags.DEFINE_string("report_dir", "reports", "JSON Test Spec")
flags.DEFINE_string("spec", "", "JSON Test Spec")
flags.DEFINE_bool("skip_input", False, "")
flags.DEFINE_bool("dry_run", False, "")
flags.DEFINE_bool("regen_report", False, "")
flags.DEFINE_bool("track_stats", False, "")
flags.DEFINE_integer("repeat", 0, "")
flags.DEFINE_bool("check_results", True, "")

runner_bin = './bench/benchmark_runner'
def build_runner(force_track_stats=False):
    track_stats='-DTRACK_STATS=ON' if (FLAGS.track_stats or force_track_stats) else '-DTRACK_STATS=OFF'
    subprocess.run(['cmake', '-B', 'bench', track_stats, '-S', '.'])
    subprocess.run(['cmake', '--build', 'bench', '-j'])

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
            (result_json['spec']['label'],
            result_json['spec']['label_x'],
            result_json['spec']['name'],
            metric_val))
    return results_table

def run(command, force_dry_run=False, prefix=''):
    if FLAGS.dry_run:
        force_dry_run = True
    if FLAGS.regen_report:
        force_dry_run = True
    command_str = " ".join(command)
    result = {"command": command_str}
    if force_dry_run:
        result["status"] = "dry_run"
        return
    print(prefix ,command_str)
    process= subprocess.run(command, capture_output=True, text=True)
    if process.returncode == 0:
        result_json = json.loads(process.stdout)
        result.update(result_json)
    else:
        result['status'] = 'NZEC'
    return result


def main(argv):
    benchmark_file = open(FLAGS.spec)
    benchmark = json.load(benchmark_file)

    if not FLAGS.regen_report:
        build_runner(("track_stats" in benchmark) and benchmark["track_stats"])

    # Create benchmark directories
    bench_dir = os.path.join(FLAGS.run_dir, os.path.splitext(FLAGS.spec)[0])
    report_dir = os.path.join(FLAGS.report_dir, os.path.splitext(FLAGS.spec)[0])
    input_dir = os.path.join(bench_dir, 'input')
    os.makedirs(bench_dir, exist_ok=True)
    os.makedirs(report_dir, exist_ok=True)
    os.makedirs(input_dir, exist_ok=True)

    input_file_map = {}
    results = {}
    # Create input sstable files.
    results['input_creation'] = []
    idx = 1
    for input in benchmark["inputs"]["list"]:
        input_json = benchmark["inputs"]["common"].copy()
        input_json.update(input)
        input_json['result_path'] = os.path.join(input_dir, input_json['name'])
        input_json_path = os.path.join(input_dir, input_json['name'] + '_spec.json')
        input_file_map[input_json['name']] = input_json['result_path']
        if "common_keys_file" in input_json:
            input_json["common_keys_file"] = input_file_map[input_json["common_keys_file"]]
        with open(input_json_path, "w") as out:
            out.write(json.dumps(input_json, indent=4))
        results['input_creation'].append(run([runner_bin, input_json_path], FLAGS.skip_input, "Generating input: %s/%s" % (idx ,len(benchmark["inputs"]["list"]))))
        idx += 1

    total_repeats = benchmark['algos']['repeat']
    if FLAGS.repeat != 0:
        total_repeats = FLAGS.repeat

    # Generate test configs. 
    # Each test is run with a different technique.
    run_configs = []
    test_results= {}
    for test in benchmark['tests']['list']:
        test_dir = os.path.join(bench_dir, test['dir'])
        os.makedirs(test_dir, exist_ok=True)
        test_results[test_dir] = []
        for technique in benchmark['algos']['list']:
            for run_repeat in range(0, total_repeats):
                test_run_json = benchmark['algos']['common'].copy()
                test_run_json.update(technique)
                test_run_json.update(test)
                test_run_json['inner_table'] = input_file_map[test_run_json['inner_table']]
                test_run_json['outer_table'] = input_file_map[test_run_json['outer_table']]
                test_run_json['result_path'] = os.path.join(test_dir, technique['name'] + '_' + str(run_repeat))
                test_results[test_dir].append(test_run_json['result_path'])
                test_run_json_path = os.path.join(test_dir, technique['name'] + '_' + str(run_repeat) + '_spec.json')
                run_configs.append(test_run_json_path)
                with open(test_run_json_path, "w") as out:
                    out.write(json.dumps(test_run_json, indent=4))

    results_table = []
    results = []

    # Shuffle the tests and run.
    random.shuffle(run_configs)
    idx = 1
    for run_config in run_configs:
        result_json = run([runner_bin, run_config], prefix="Running %s/%s" % (idx, len(run_configs)))
        results.append(result_json)
        idx += 1
    
    benchmark_data_json = os.path.join(bench_dir, 'run_results.json')
    if FLAGS.regen_report:
        with open(benchmark_data_json, "r") as out:
            results = json.load(out)

    with open(benchmark_data_json, "w") as out:
        out.write(json.dumps(results, indent=4))
    
    if FLAGS.check_results:
        for test_result in test_results:
            result_files = test_results[test_result]
            results_differ = False
            for idx in enumerate(result_files[1:]):
                if not filecmp.cmp(result_files[idx[0]], result_files[idx[0]-1]):
                    results_differ = True
            if results_differ:
                print(os.path.join(bench_dir, test['dir']) + " DIFF: FAIL")
            else:
                print(test['dir'] + " DIFF: OK")



    report_lines = []
    for metric_fields in benchmark["metrics"]:
        report_lines.append("### " + metric_fields[-1] + "\n\n")
        results_table = extract_table(results, metric_fields)
        df = pd.DataFrame.from_records(results_table)
        grouped = df.groupby([0, 1, 2]).agg(metric=(3, 'median')).reset_index()
        report_lines.append(grouped.pivot(index=1, columns=2, values='metric').to_markdown() + "\n\n")
    
    report = os.path.join(report_dir, 'report.md')
    print(report)
    with(open(report, 'w')) as f:
        f.writelines(report_lines)
    exit()

if __name__ == "__main__":
    app.run(main)
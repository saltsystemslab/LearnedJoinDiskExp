#!/usr/bin/python3
import sys
import json
import os
import shutil
import subprocess
import pprint
import random
import pandas as pd

runner_bin = './bench_release/benchmark_runner'

def extract_table(results, metric_fields):
    results_table = []
    for result_json in results:
        metric_dict = result_json
        for field in metric_fields[:-1]:
            metric_dict = metric_dict[field]
        metric_val = None
        if metric_dict and metric_fields[-1] in metric_dict:
            metric_val = metric_dict[metric_fields[-1]]
        results_table.append(
            (result_json['spec']['label'],
            result_json['spec']['label_x'],
            result_json['spec']['name'],
            metric_val))
    return results_table

def main():
    if len(sys.argv) < 2:
        print("Please specify input benchmark JSON spec")
        exit()
    subprocess.run(['cmake', '--clean', 'bench_release'])
    subprocess.run(['cmake', '-B', 'bench_release', '-DCMAKE_BUILD_TYPE=Debug', '-S', '.'])
    subprocess.run(['cmake', '--build', 'bench_release', '-j', '6'])
    benchmark_file = open(sys.argv[1])
    benchmark = json.load(benchmark_file)

    # Create benchmark directories
    bench_dir = os.path.join(benchmark['dir'])
    input_dir = os.path.join(bench_dir, 'input')
    shutil.rmtree(bench_dir, ignore_errors=True)
    os.makedirs(bench_dir)
    os.makedirs(input_dir)
    input_file_map = {}
    results = {}

    # Create input sstable files.
    results['input_creation'] = []
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
        result = subprocess.run([runner_bin, input_json_path], capture_output=True, text=True)
        print("Generating input: " + input_json_path)
        print("STDOUT: " + result.stdout)
        print("STDERR: " + result.stderr)
        result_json = json.loads(result.stdout)
        results['input_creation'].append(result_json)

    columns = ['label', 'label_x']
    # Run tests. Each test is run with a different technique.
    results['test_results'] = []
    run_configs = []
    for test in benchmark['tests']['list']:
        test_dir = os.path.join(bench_dir, test['dir'])
        os.makedirs(test_dir)
        test_results= []
        for technique in benchmark['algos']['list']:
            for run_repeat in range(0, benchmark['algos']['repeat']):
                test_run_json = benchmark['algos']['common'].copy()
                test_run_json.update(technique)
                test_run_json.update(test)
                test_run_json['inner_table'] = input_file_map[test_run_json['inner_table']]
                test_run_json['outer_table'] = input_file_map[test_run_json['outer_table']]
                test_run_json['result_path'] = os.path.join(test_dir, technique['name'] + '_' + str(run_repeat))
                test_run_json_path = os.path.join(test_dir, technique['name'] + '_' + str(run_repeat) + '_spec.json')
                run_configs.append(test_run_json_path)
                with open(test_run_json_path, "w") as out:
                    out.write(json.dumps(test_run_json, indent=4))


    results_table = []
    results = []
    random.shuffle(run_configs)
    for run_config in run_configs:
        result = subprocess.run([runner_bin, run_config], capture_output=True, text=True)
        print("RunConfig: " + run_config)
        print("Output:" + result.stdout)
        result_json = json.loads(result.stdout)
        results.append(result_json)

    benchmark_data_json = os.path.join(bench_dir, 'run_report.json')
    with open(benchmark_data_json, "w") as out:
        out.write(json.dumps(results, indent=4))

    results_table = extract_table(results, ('result', 'duration_sec'))
    df = pd.DataFrame.from_records(results_table)
    grouped = df.groupby([0, 1, 2]).agg(median_duration=(3, 'median'), variance=(3, 'var')).reset_index()
    print(grouped)
    print(grouped.columns)
    print(grouped.pivot(index=1, columns=2, values='median_duration'))

    results_table = extract_table(results, ('result', 'merge_log', 'max_index_error_correction'))
    df = pd.DataFrame.from_records(results_table)
    grouped = df.groupby([0, 1, 2]).agg(max_error_correction=(3, 'median'), variance=(3, 'var')).reset_index()
    print(grouped.pivot(index=1, columns=2, values='max_error_correction'))

    results_table = extract_table(results, ('result', 'inner_index_size'))
    df = pd.DataFrame.from_records(results_table)
    grouped = df.groupby([0, 1, 2]).agg(inner_index_size=(3, 'median'), variance=(3, 'var')).reset_index()
    print(grouped.pivot(index=1, columns=2, values='inner_index_size'))

    results_table = extract_table(results, ('result', 'merge_log', 'comparison_count'))
    df = pd.DataFrame.from_records(results_table)
    grouped = df.groupby([0, 1, 2]).agg(max_error_correction=(3, 'median'), variance=(3, 'var')).reset_index()
    print(grouped.pivot(index=1, columns=2, values='max_error_correction'))

if __name__ == "__main__":
    main()
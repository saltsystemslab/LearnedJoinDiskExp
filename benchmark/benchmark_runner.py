#!/usr/bin/python3
import sys
import json
import os
import shutil
import subprocess
import pprint

runner_bin = './build/benchmark_runner'

def main():
    if len(sys.argv) < 2:
        print("Please specify input benchmark JSON spec")
        exit()
    benchmark_file = open(sys.argv[1])
    benchmark = json.load(benchmark_file)

    # Create input directories
    bench_dir = os.path.join(benchmark['dir'])
    input_dir = os.path.join(bench_dir, 'input')

    shutil.rmtree(bench_dir, ignore_errors=True)
    os.makedirs(bench_dir)
    os.makedirs(input_dir)
    input_file_map = {}
    results = {}
    results['input_creation'] = []
    for input in benchmark["inputs"]["list"]:
        input_json = input.copy()
        input_json.update(benchmark["inputs"]["common"])
        input_json['result_path'] = os.path.join(input_dir, input_json['name'])
        input_json_path = os.path.join(input_dir, input_json['name'] + '_spec.json')
        input_file_map[input_json['name']] = input_json['result_path']
        with open(input_json_path, "w") as out:
            out.write(json.dumps(input_json, indent=4))
        result = subprocess.run([runner_bin, input_json_path], capture_output=True, text=True)
        result_json = json.loads(result.stdout)
        results['input_creation'].append(result_json)
    
    results['test_results'] = {}
    for test in benchmark['tests']['list']:
        test_dir = os.path.join(bench_dir, test['dir'])
        os.makedirs(test_dir)
        test_results= []
        for technique in benchmark['algos']['list']:
            test_run_json = benchmark['algos']['common'].copy()
            test_run_json.update(technique)
            test_run_json.update(test)
            test_run_json['inner_table'] = input_file_map[test_run_json['inner_table']]
            test_run_json['outer_table'] = input_file_map[test_run_json['outer_table']]
            test_run_json['result_path'] = os.path.join(test_dir, technique['name'])
            test_run_json_path = os.path.join(test_dir, technique['name'] + '_spec.json')
            with open(test_run_json_path, "w") as out:
                out.write(json.dumps(test_run_json, indent=4))
            result = subprocess.run([runner_bin, test_run_json_path], capture_output=True, text=True)
            result_json = json.loads(result.stdout)
            test_results.append(result_json)
        results['test_results'][test['label']] = test_results;
    benchmark_data_json = os.path.join(bench_dir, 'run_report.json')
    with open(benchmark_data_json, "w") as out:
        out.write(json.dumps(results, indent=4))

if __name__ == "__main__":
    main()
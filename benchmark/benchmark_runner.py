#!/usr/bin/python3
import sys
import json
import os
import shutil
import subprocess

runner_bin = './build/benchmark_runner'

def main():
    if len(sys.argv) < 2:
        print("Please specify input benchmark JSON spec")
        exit()
    benchmark_file = open(sys.argv[1])
    benchmark = json.load(benchmark_file)

    # Create input directories
    shutil.rmtree(benchmark['dir'])
    os.makedirs(benchmark['dir'])

    for input in benchmark["inputs"]["sstables"]:
        input_json = input.copy()
        input_json.update(benchmark["inputs"]["common"])
        input_json['path'] = os.path.join(benchmark['dir'], input_json['name'])
        input_json_path = os.path.join(benchmark['dir'], input_json['name'] + '_spec.json')
        with open(input_json_path, "w") as out:
            out.write(json.dumps(input_json, indent=4))
        subprocess.run([runner_bin, input_json_path])

if __name__ == "__main__":
    main()
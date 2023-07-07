#!/usr/bin/python3

import os
import subprocess
import sys
import time

ratio = [1, 10, 50, 60, 80, 100]
common_keys = [0.1]
merge_modes = ["standard", "learned", "parallel_learned", "parallel_standard", "standard_join", "learned_join"]
num_threads = [8]
key_sizes = [(8, 40_000) , (16, 20_000), (32, 10_000)]
plr_errors = [2, 10, 100, 1000]
lim = 610000000 * 8


def generate_string_key_runs(test_dir, modes):
    env = os.environ

    runs = [["numactl", "-N", "1", "-m", "1", "./bin/benchmark"]]
    all_runs = []
    for run in runs:
        for key_size in key_sizes:
            for r in ratio:
                    run_args = run.copy()
                    run_args.append("--num_keys=%s,%s" % (key_size[1], key_size[1]*r))
                    run_args.append("--key_bytes=%s" % (key_size[0]))
                    run_args.append("--key_type=uint64")
                    all_runs.append(run_args)

    runs = all_runs.copy()
    all_runs = []
    for run in runs:
        for merge_mode in modes:
            run_args = run.copy()
            run_args.append("--merge_mode=%s" % merge_mode)
            if (merge_mode.startswith("parallel")):
                run_args.append("--num_threads=%s" % (4))
            if "join" in merge_mode:
                run_args.append("--num_common_keys=%s" % int(key_size[1] * 0.001))
            all_runs.append(run_args)

    runs = all_runs.copy()
    all_runs = []
    for run in runs:
        run_args = run.copy()

        is_learned = False
        for arg in run_args:
            if "learned" in arg:
                is_learned = True

        if not is_learned:
            all_runs.append(run_args)
            continue

        for plr_error in plr_errors:
            run_args = run.copy()
            run_args.append("--plr_error_bound=%s" % plr_error)

            all_runs.append(run_args)

    runs = all_runs.copy()
    all_runs = []
    if 'no_op' not in modes:
        for run in runs:
            run_args = run.copy()
            run_args.append("--use_disk=0")
            all_runs.append(run_args)

            run_args = run.copy()
            run_args.append("--use_disk=1")
            all_runs.append(run_args)

    for run in all_runs:
        run.append('--test_dir=%s' % test_dir)
    return all_runs


test_run_name = sys.argv[1]
os.system("make clean benchmark")
os.system("mkdir -p test_runs")
test_dir = "./test_runs/"
runs = generate_string_key_runs(test_dir+test_run_name, ["no_op"])
for run in runs:
    print('###############')
    print('run: %s', str(run))
    result = subprocess.run(run, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print('####STDERR#####')
        print(result.stderr)
        print('###############')
    print('###############')
    sys.stdout.flush()

runs = generate_string_key_runs(test_dir + test_run_name, merge_modes)
import random
random.shuffle(runs)
print(len(runs))
for run in runs:
    print('###############')
    print('run: %s', str(run))
    sys.stdout.flush()
    subprocess.run(run)

#!/usr/bin/python

import os
import subprocess

ratio = [1, 10, 30, 50, 60, 80, 100]
common_keys = [0.001]
merge_modes = ["standard", "learned", "parallel_learned", "parallel_standard", "standard_join", "learned_join", "learned_binary_join"]
num_threads = [4]
key_sizes = [(8, 40_000_000) , (16, 20_000_000), (32, 10_000_000)]
# key_sizes = [(8, 40_000) , (16, 20_000), (32, 10_000)]
plr_errors = [2, 10, 50]
lim = 610000000 * 8


def generate_string_key_runs(test_dir, modes):
    env = os.environ
    env["USE_STRING_KEYS"] = "0"
    env["USE_INT_128"] = "0"

    subprocess.run(["make", "clean", "benchmark"], env=env)

    runs = [["./bin/benchmark"]]
    all_runs = []
    for run in runs:
        for key_size in key_sizes:
            for r in ratio:
                for common_key in common_keys:
                    run_args = run.copy()
                    run_args.append("--num_keys=%s,%s" % (key_size[1], key_size[1]*r))
                    run_args.append("--key_bytes=%s" % (key_size[0]))
                    run_args.append("--num_common_keys=%s" % int(key_size[1] * common_key))
                    all_runs.append(run_args)

    runs = all_runs.copy()
    all_runs = []
    for run in runs:
        for merge_mode in modes:
            run_args = run.copy()
            run_args.append("--merge_mode=%s" % merge_mode)
            if (merge_mode.startswith("parallel")):
                run_args.append("--num_threads=%s" % (4))
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
    for run in runs:
        run_args = run.copy()
        run_args.append("--use_disk=0")
        all_runs.append(run_args)

        run_args = run.copy()
        run_args.append("--use_disk=1")
        all_runs.append(run_args)

    for run in runs:
        summary=str(run).replace(' ', '_')
        run.append('--test_dir=%s' % test_dir)

    return runs




os.system("mkdir -p test_runs")
dir = "./test_runs/"
runs = generate_string_key_runs("./test_runs/run", ["no_op"])
for run in runs:
    print("run_type: %s " % run)
    subprocess.run(run)

runs = generate_string_key_runs("./tests/sample_run", merge_modes)
print(len(runs))
for run in runs:
    subprocess.run(run)
    



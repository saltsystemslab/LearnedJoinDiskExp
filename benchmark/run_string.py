import subprocess

benchmark_script = "./scripts/benchmark.py"

# Datasets with 200M items. Single Thread. Run 3 times. 10,20,...100
datasets = {
    "str": {
        "num_keys": 100000000
    },
}

threads = [1, 4, 8, 32]
for thread in threads:
    for name, dataset in datasets.items():
        join_args = [benchmark_script, "--spec=benchmark/string_join.jsonnet"] 
        args = join_args
        args.append(f"--threads={thread}")
        args.append(f"--repeat=1")
        args.append(f"--test_name=join_{name}")
        args.append(f'--clear_inputs=False')
        args.append(f'--string_keys=True')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        subprocess.run(args)

        merge_args = [benchmark_script, "--spec=benchmark/string_merge.jsonnet"] 
        args = merge_args
        args.append(f"--threads={thread}")
        args.append(f"--repeat=2")
        args.append(f"--test_name=merge_{name}")
        args.append(f'--clear_inputs=False')
        args.append(f'--string_keys=True')
        args.append(f'--sosd_num_keys={dataset["num_keys"]}')
        subprocess.run(args)
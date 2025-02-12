#!/usr/bin/python3
import json
import os
import subprocess
import random
import shutil
import pandas as pd
from absl import app
from absl import flags
import filecmp
import asyncio
import _jsonnet

FLAGS = flags.FLAGS

flags.DEFINE_string("spec", "", "JSON Test Spec")
flags.DEFINE_string("test_dir", "sponge", "JSON Test Spec")
flags.DEFINE_bool("only_build", False, "Only build")
flags.DEFINE_bool("dry_run", False, "Skip run")
flags.DEFINE_bool("skip_input", False, "Skip input creation")
flags.DEFINE_bool("only_input", False, "Create input and exit")
flags.DEFINE_bool("check_results", False, "Verify that all the outputs are same.")
flags.DEFINE_bool("track_stats", False, "Use debug build and count microbenchmark stats.")
flags.DEFINE_bool("string_keys", False, "Use string keys.")
flags.DEFINE_bool("use_alex", False, "Use alex.")
flags.DEFINE_bool("debug_build", False, "")
flags.DEFINE_bool("use_numactl", True, "")
flags.DEFINE_bool("use_cgroups", False, "")
flags.DEFINE_bool("clear_fs_cache", False, "")
flags.DEFINE_string("cgroup_name", "", "")
flags.DEFINE_integer("repeat", 3, "")
flags.DEFINE_integer("threads", 1, "")
flags.DEFINE_bool("regen_report", False, "")
flags.DEFINE_bool("clean", False, "")
flags.DEFINE_bool("clear_inputs", False, "")
flags.DEFINE_string("test_name", "unknown", "Test Case Name.")
flags.DEFINE_string("sosd_source", "unknown", "SOSD source dataset file")
flags.DEFINE_integer("sosd_num_keys", 100000000, "Num Keys in SOSD")
flags.DEFINE_string("ratios", "[1,10,100,100]", "")
flags.DEFINE_string("indexes", "[\"btree256\",\"sampledflatpgm256\"]","")
flags.DEFINE_string("non_indexed_joins", "[\"sort_join\",\"hash_join\"]","")
flags.DEFINE_string("indexed_joins", "[\"lsj\",\"inlj\"]","")
flags.DEFINE_string("io_device", "nvme0n1", "")
flags.DEFINE_string("cgroup", "/sys/fs/cgroup/user.slice", "")

def main(argv):
    print(FLAGS.test_dir)
    os.makedirs(FLAGS.test_dir, exist_ok=True)
    runner_bin = build_runner(os.path.join(FLAGS.test_dir, 'build')); 
    # Setup Experiment Directories
    exp = setup_experiment_directories()
    if FLAGS.only_build:
        return

    # Generate the experiment json config.
    # This config has definitions to generate input files, and test configurations.
    print(FLAGS.spec)
    print("RATIOS: ", FLAGS.ratios)
    print(FLAGS.indexes)
    print(FLAGS.indexed_joins)
    print(FLAGS.non_indexed_joins)
    exp['config'] = json.loads(_jsonnet.evaluate_file(
        FLAGS.spec, ext_vars = {
            "TEST_OUTPUT_DIR": exp['output_dir'],
            "TEST_INPUT_DIR": exp['input_dir'],
            "TEST_REPEAT": str(FLAGS.repeat),
            "TEST_NUM_THREADS": str(FLAGS.threads),
            "TEST_DATASET_SIZE": str(FLAGS.sosd_num_keys),
            "TEST_DATASET_SOURCE": str(FLAGS.sosd_source),
            "TEST_DATASET_NAME": str(os.path.basename(FLAGS.sosd_source)),
            "TEST_CHECK_CHECKSUM": str(FLAGS.check_results),
            "TEST_RATIOS": str(FLAGS.ratios),
            "TEST_INDEXES": str(FLAGS.indexes),
            "TEST_NON_INDEXED_JOINS": str(FLAGS.non_indexed_joins),
            "TEST_INDEXED_JOINS": str(FLAGS.indexed_joins)
        }
    ))

    # Benchmark Runner takes in a JSON config to either 
    #   a) generate an input SSTable
    #   b) run an experiment in a test (eg. join input 1 and input 2 using pgm)
    # Generate all the JSON configs.
    generate_configs(exp['config']['inputs'], exp['input_config_dir'])
    generate_configs(exp['config']['tests'], exp['output_config_dir'])
    run_configs(runner_bin, exp['input_config_dir'], exp['input_result_dir'], shuffle=False, dry_run=FLAGS.skip_input or FLAGS.dry_run, use_cgroups=False)
    run_configs(runner_bin, exp['output_config_dir'], exp['output_result_dir'], shuffle=True, total_repeat=FLAGS.repeat, delete_result_path=True, dry_run=FLAGS.only_input or FLAGS.dry_run, use_cgroups=FLAGS.use_cgroups)
    if FLAGS.clear_inputs and not FLAGS.only_input and not FLAGS.dry_run:
        shutil.rmtree(exp['input_dir'], ignore_errors=True)

def build_runner(build_dir, force_track_stats=False):
    track_stats='-DTRACK_STATS=ON' if (FLAGS.track_stats or force_track_stats) else '-DTRACK_STATS=OFF'
    string_keys = '-DSTRING_KEYS=ON' if (FLAGS.string_keys) else '-DSTRING_KEYS=OFF'
    debug = '-DCMAKE_BUILD_TYPE=debug' if (FLAGS.debug_build) else '-DCMAKE_BUILD_TYPE=release'
    use_alex = '-DUSE_ALEX=ON' if (FLAGS.use_alex) else '-DUSE_ALEX=OFF'
    subprocess.run(['cmake', '-B', build_dir, track_stats, string_keys, debug, use_alex, '-S', '.'])
    subprocess.run(['cmake', '--build', build_dir, '-j'])
    return os.path.join(build_dir, 'benchmark_runner')

def get_specname():
    spec_path_with_extension = os.path.split(FLAGS.spec)[-1]
    return os.path.splitext(spec_path_with_extension)[0]

def setup_experiment_directories():
    experiment_dir = os.path.join(FLAGS.test_dir, FLAGS.test_name + "_threads="+str(FLAGS.threads))
    # Input json spec configs, where to generate them, where to store outputs.
    input_config_dir = os.path.join(experiment_dir, "input_configs")
    input_dir = os.path.join(experiment_dir, "inputs")
    input_result_dir = os.path.join(experiment_dir, "input_results")
    # Output json spec configs, where to generate them, where to store outputs.
    output_config_dir = os.path.join(experiment_dir, "output_configs")
    output_dir = os.path.join(experiment_dir, "outputs")
    output_result_dir = os.path.join(output_dir, "results")
    csv_dir = os.path.join(experiment_dir, "csv")
    shutil.rmtree(input_config_dir, ignore_errors=True)
    shutil.rmtree(output_config_dir, ignore_errors=True)
    if FLAGS.clean:
        shutil.rmtree(experiment_dir, ignore_errors=True)
        shutil.rmtree(input_dir, ignore_errors=True)
        shutil.rmtree(output_dir, ignore_errors=True)
        shutil.rmtree(input_result_dir, ignore_errors=True)
        shutil.rmtree(output_result_dir, ignore_errors=True)
        shutil.rmtree(csv_dir, ignore_errors=True)

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

def get_iostat():
    command = ['iostat', '-o', 'JSON', '-p', FLAGS.io_device]
    process = subprocess.run(command, capture_output=True, text=True)
    iostat = json.loads(process.stdout)
    disk_statistics = iostat["sysstat"]["hosts"][0]["statistics"][0]["disk"]
    iostat = {}
    for disk in disk_statistics:
        if disk["disk_device"] == FLAGS.io_device:
            iostat["bytes_read"] = disk["kB_read"] * 1000
            iostat["bytes_wrtn"] = disk["kB_wrtn"] * 1000
    return iostat

def get_cgroup_iostat():
    command = ['cat', os.path.join(FLAGS.cgroup,'io.stat')]
    process = subprocess.run(command, capture_output=True, text=True)
    tokens = process.stdout.split(' ')
    iostat = {}
    print(tokens)
    for i in range(0, len(tokens)):
        if tokens[i].startswith('rbytes'):
            iostat['bytes_read'] = int(tokens[i].split("=")[1])
        if tokens[i].startswith('wbytes'):
            iostat['bytes_wrtn'] = int(tokens[i].split("=")[1])
    print(iostat)
    return iostat

def run_configs(runner_bin, config_dir, result_dir, shuffle=True, total_repeat=1, delete_result_path=False, dry_run=False, use_cgroups=False):
        print("Running configs in ", config_dir)
        configs = os.listdir(config_dir)
        if shuffle:
            random.shuffle(configs)
        else:
            configs = sorted(configs)

        run_result_dir = os.path.join(result_dir, f'run')
        os.makedirs(run_result_dir, exist_ok=True)

        for config in configs:
            result_json = run([runner_bin, os.path.join(config_dir, config)], prefix="Running %s" % config, use_cgroups=use_cgroups, dry_run=dry_run)
            if dry_run:
                continue
            if delete_result_path:
                print(result_json)
                os.remove(result_json['spec']['result_path'])
            with open(os.path.join(result_dir, config), "w") as outfile:
                result_json = json.dumps(result_json, indent=4)
                outfile.write(result_json)

def run(command, prefix='', use_cgroups=False, dry_run=False):
    if FLAGS.regen_report:
        return
    if FLAGS.use_numactl:
        command = ['numactl', '-N', '0', '-m', '0'] + command
    if FLAGS.clear_fs_cache:
        clear_command = ['echo 1 | sudo tee /proc/sys/vm/drop_caches',]
        subprocess.run(clear_command, shell=True, text=True)
    
    command_str = " ".join(command)
    result = {"command": command_str}
    print(prefix, ' '.join(command))
    process = {}

    io_stats_before = get_iostat()
    cgroup_io_stats_before = {}
    cgroup_io_stats_after = {}
    if dry_run:
        return
    elif use_cgroups:
        cgroup_io_stats_before = get_cgroup_iostat()
        cgroup_proc_path = os.path.join(FLAGS.cgroup, 'cgroup.procs')
        command = [f'echo $$ >> {cgroup_proc_path} && ' + command_str]
        process = subprocess.run(command, shell=True, capture_output=True, text=True)
        cgroup_io_stats_after = get_cgroup_iostat()
    else:
        process = subprocess.run(command, capture_output=True, text=True)

    io_stats_after = get_iostat()

    if process.returncode == 0:
        result_json = json.loads(process.stdout)
        result.update(result_json)
        result['iostat'] = {}
        result['iostat']['bytes_read'] = io_stats_after['bytes_read'] - io_stats_before['bytes_read']
        result['iostat']['bytes_wrtn'] = io_stats_after['bytes_wrtn'] - io_stats_before['bytes_wrtn']
        if use_cgroups:
            result['cgroup_iostat'] = {}
            result['cgroup_iostat']['bytes_read'] = cgroup_io_stats_after['bytes_read'] - cgroup_io_stats_before['bytes_read']
            result['cgroup_iostat']['bytes_wrtn'] = cgroup_io_stats_after['bytes_wrtn'] - cgroup_io_stats_before['bytes_wrtn']

    else:
        print(process.stdout, process.stderr)
        result['status'] = 'NZEC'
    return result


def remove_result(result_path):
    try:
        os.remove(result_path)
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    app.run(main)

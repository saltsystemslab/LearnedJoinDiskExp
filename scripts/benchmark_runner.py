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

FLAGS = flags.FLAGS

flags.DEFINE_string("run_dir", "test_runs", "JSON Test Spec")
flags.DEFINE_string("csv_dir", "csv", "CSV")
flags.DEFINE_string("report_dir", "reports", "JSON Test Spec")
flags.DEFINE_string("spec", "", "JSON Test Spec")
flags.DEFINE_bool("skip_input", False, "")
flags.DEFINE_bool("dry_run", False, "")
flags.DEFINE_bool("regen_report", False, "")
flags.DEFINE_bool("track_stats", False, "")
flags.DEFINE_bool("string_keys", False, "")
flags.DEFINE_bool("debug_build", False, "")
flags.DEFINE_integer("repeat", 0, "")
flags.DEFINE_bool("check_results", True, "")

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
    if FLAGS.dry_run:
        force_dry_run = True
    if FLAGS.regen_report:
        force_dry_run = True
    command = ['numactl', '-N', '1', '-m', '1'] + command
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

def remove_result(result_path):
    try:
        os.remove(result_path)
    except FileNotFoundError:
        pass

def main(argv):
    benchmark_file = open(FLAGS.spec)
    benchmark = json.load(benchmark_file)

    if not FLAGS.regen_report:
        build_runner(("track_stats" in benchmark) and benchmark["track_stats"])

    # Create benchmark directories
    csv_dir = os.path.join(FLAGS.csv_dir, os.path.splitext(FLAGS.spec)[0])
    bench_dir = os.path.join(FLAGS.run_dir, os.path.splitext(FLAGS.spec)[0])
    report_dir = os.path.join(FLAGS.report_dir, os.path.splitext(FLAGS.spec)[0])
    input_dir = os.path.join(bench_dir, 'input')
    os.makedirs(csv_dir, exist_ok=True)
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

    total_repeats = benchmark["repeat"] if "repeat" in benchmark else 1
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
                run_configs.append({
                    'spec_path': test_run_json_path, 
                    'result_path': test_run_json['result_path']
                })
                with open(test_run_json_path, "w") as out:
                    out.write(json.dumps(test_run_json, indent=4))

    results_table = []
    results = []

    # Shuffle the tests and run.
    random.shuffle(run_configs)
    idx = 1
    for run_config in run_configs:
        result_json = run([runner_bin, run_config['spec_path']], prefix="Running %s/%s" % (idx, len(run_configs)))
        if not FLAGS.dry_run:
            remove_result(run_config['result_path'])
        results.append(result_json)
        idx += 1
    
    benchmark_data_json = os.path.join(bench_dir, 'run_results.json')
    if FLAGS.regen_report or FLAGS.dry_run:
        with open(benchmark_data_json, "r") as out:
            results = json.load(out)

    with open(benchmark_data_json, "w") as out:
        out.write(json.dumps(results, indent=4))
    
    if FLAGS.check_results:
        result_checksums = {}
        for result in results:
            result_path = result['spec']['result_path']
            result_dir = os.path.dirname(result_path)
            if result_dir not in result_checksums:
                result_checksums[result_dir] = []
            result_checksums[result_dir].append(result['result']['checksum'])

        for result_dir in result_checksums:
            diff_found = False
            for idx, checksum in enumerate(result_checksums[result_dir][1:]):
                if checksum != result_checksums[result_dir][idx-1]:
                    diff_found = True
            if diff_found:
                print(result_dir + "DIFF: FAIL")
            else:
                print(result_dir + "DIFF: OK")

    report_lines = []
    # Bunch of hardcoding for generating graphs
    fig, axs = plt.subplots(2, 3, figsize=(30, 16))
    fig.tight_layout(pad=4.0)
    for metric_fields in benchmark["metrics"]:
        report_lines.append("### " + metric_fields[-1] + "\n\n")
        results_table = extract_table(results, metric_fields)
        df = pd.DataFrame.from_records(results_table)
        grouped = df.groupby([0, 1]).agg(metric=(2, 'mean')).reset_index()
        pivot = grouped.pivot(index=0, columns=1, values='metric')
        pivot.columns = pivot.columns.str.replace("_","-")
        if metric_fields[-1] == "duration_sec":
            for c in pivot.columns:
                axs[0][0].plot(pivot.index, pivot[c], label=c, marker='.')
            axs[0][0].set_xlabel("fraction")
            axs[0][0].set_ylabel("duration(s)")
            axs[0][0].legend()
            axs[0][0].set_title("Absolute Latency")
            columns = pivot.columns
            rel_columns = []
            if ('sort-join' not in columns):
                continue
            for c in columns:
                pivot[c + '-relative'] = (100.0 * (pivot['sort-join'] - pivot[c]) / pivot['sort-join'])
                rel_columns.append(c + '-relative')
                axs[0][1].plot(pivot.index, pivot[c+'-relative'], label=c, marker='.')
                axs[0][1].set_ylim([-100, 100])
            print(pivot[rel_columns])
            pivot[rel_columns].to_csv(os.path.join(csv_dir, metric_fields[-1] + "-rel.csv"))
            axs[0][1].set_xlabel("fraction")
            axs[0][1].set_ylabel("rel % to sort-join")
            axs[0][1].legend()
            axs[0][1].set_title("Inner Index Size")
        elif metric_fields[-1] == "inner_index_size":
            axs[0][2].bar(pivot.columns, pivot.iloc[0], log=True)
            # t = pivot[['BTree', 'PGM-8', 'PGM-64', 'PGM-128']].transpose()
            t = pivot[['PGM-8', 'PGM-64', 'PGM-128', 'PGM-256', 'PGM-1024']].transpose()
            t['Size'] = t[t.columns[0]]
            t['Size'].to_csv(os.path.join(csv_dir, "inner_index_transposed.csv"))
            axs[0][2].set_xlabel("Index")
            axs[0][2].set_ylabel("In Memory Size(B)")
            axs[0][2].set_title("Inner Index Size")
        elif metric_fields[-1] == "inner_index_build_duration_sec":
            axs[1][2].bar(pivot.columns, pivot.iloc[0], log=True)
            t = pivot[['PGM-8', 'PGM-64', 'PGM-128', 'PGM-256', 'PGM-1024']].transpose()
            t['Size'] = t[t.columns[0]]
            t['Size'].to_csv(os.path.join(csv_dir, "inner_index_build_time_transposed.csv"))
            axs[1][2].set_xlabel("Index")
            axs[1][2].set_ylabel("Duration[sec]")
            axs[1][2].set_title("Index construction time")
        elif metric_fields[-1] == "outer_index_build_duration":
            for c in pivot.columns:
                axs[0][0].plot(pivot.index, pivot[c], label=c, marker='.')
            axs[0][0].set_xlabel("fraction")
            axs[0][0].set_ylabel("Outer Index Build Duration")
            axs[0][0].legend()
        elif metric_fields[-1] == "inner_index_build_duration":
            for c in pivot.columns:
                axs[0][1].plot(pivot.index, pivot[c], label=c, marker='.')
            axs[0][1].set_xlabel("fraction")
            axs[0][1].set_ylabel("Inner Index Build Duration")
            axs[0][1].legend()
        elif metric_fields[-1] == "inner_disk_fetch":
            for c in pivot.columns:
                axs[1][0].plot(pivot.index, pivot[c], label=c, marker='.')
            axs[1][0].set_xlabel("fraction")
            axs[1][0].set_ylabel("4k block fetches")
            axs[1][0].legend()
            axs[1][0].set_title("4k block fetches (inner table)")
        elif metric_fields[-1] == "outer_disk_fetch":
            for c in pivot.columns:
                axs[1][1].plot(pivot.index, pivot[c], label=c, marker='.')
            axs[1][1].set_xlabel("Fraction")
            axs[1][1].set_ylabel("4K block fetches")
            axs[1][1].legend()
            axs[1][1].set_title("4K block fetches (Outer Table)")
        report_lines.append(grouped.pivot(index=0, columns=1, values='metric').to_markdown() + "\n\n")
        grouped.pivot(index=0, columns=1, values='metric').to_csv(os.path.join(csv_dir, metric_fields[-1] + ".csv"))
    tikzplotlib_fix_ncols(plt.gcf())
    tikzplotlib.save(os.path.join(report_dir, "plot.tex"))
    plt.savefig(os.path.join(report_dir,  "plots.png"))
    plt.close()
    report_lines.append("![plot.png](plot.png)\n\n") # % (metric_fields[-1], metric_fields[-1]))
    
    report = os.path.join(report_dir, 'report.md')
    print(report)
    with(open(report, 'w')) as f:
        f.writelines(report_lines)
    exit()

if __name__ == "__main__":
    app.run(main)

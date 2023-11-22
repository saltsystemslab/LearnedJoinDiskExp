#!/usr/bin/python3
import os
import json
import pandas as pd
import shutil

dstDir = '../LearnedMergePlots/experiment-nov11'

def generateReport(name, test_cases):
    os.makedirs(os.path.join(dstDir, name), exist_ok=True)
    inner_index_size = []
    inner_index_build_duration = []
    for test_case in test_cases:
        test_case_properties = test_cases[test_case]
        test_dir = os.path.join(test_case_properties['dir'])
        results_dir = os.path.join(test_dir, 'outputs', 'results')
        csv_dir = os.path.join(test_dir, 'csv')
        os.makedirs(csv_dir, exist_ok=True)
        df = buildDataFrame(results_dir)
        inner_index_size.append(getInnerIndexSizeForTestCase(df))
        inner_index_build_duration.append(getInnerIndexBuildDurationForTestCase(df))
        generateReportForTestCase(test_case, df, csv_dir)
    pd.DataFrame.from_records(inner_index_size, index=test_cases.keys()).to_csv(os.path.join(dstDir, name,  'innerIndexSize.csv'))
    pd.DataFrame.from_records(inner_index_build_duration, index = test_cases.keys()).to_csv(os.path.join(dstDir, name, 'innerIndexBuildDuration.csv'))
    
def getInnerIndexSizeForTestCase(test_dataframe):
    inner_index_size = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.inner_index_size', aggfunc='median')
    data = {}
    for index in inner_index_size:
        data[index] = inner_index_size[index].mean()
    return data

def parseIndexName(name):
    index = name
    epsilon = 0
    if name=="btree1024":
        index="BTree"
        epsilon=1024
    if name=="btree2048":
        index="BTree"
        epsilon=2048
    if name=="btree256":
        index="BTree"
        epsilon=256
    if name=="pgm1024":
        index="PGM"
        epsilon=2049
    if name=="pgm512":
        index="PGM"
        epsilon=1025
    if name=="pgm128":
        index="PGM"
        epsilon=257
    return {"name": index, "epsilon": epsilon}

def getInnerIndexSizeByEpsilonForTestCase(test_dataframe):
    inner_index_size = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.inner_index_size', aggfunc='median')
    data = []
    for index in inner_index_size:
        index_dict = parseIndexName(index)
        index_dict['memory'] = inner_index_size[index].mean() / (1024.0 * 1024.0)
        data.append(index_dict)
    return pd.DataFrame(data).pivot(index='epsilon', columns='name',values='memory')

def getInnerIndexBuildDurationForTestCase(test_dataframe):
    inner_index_build_duration = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.inner_index_build_duration_ns', aggfunc='median')
    data = {}
    for index in inner_index_build_duration:
        data[index] = inner_index_build_duration[index].mean()
    return data

def getInnerIndexBuildDurationByEpsilonForTestCase(test_dataframe):
    inner_index_build_duration = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.inner_index_build_duration_ns', aggfunc='median')
    data = []
    for index in inner_index_build_duration:
        index_dict = parseIndexName(index)
        index_dict['build_duration'] = (inner_index_build_duration[index].mean() / 1000000000.0)
        data.append(index_dict)
    return pd.DataFrame(data).pivot(index='epsilon', columns='name',values='build_duration')

def buildDataFrame(results_dir):
    runs = [os.path.join(results_dir, run) for run in os.listdir(results_dir)]
    test_results = []
    for run in runs:
        for test_result_file in os.listdir(run):
            json_file = open(os.path.join(run, test_result_file))
            test_result = json.load(json_file)
            test_result['run'] = run
            test_results.append(test_result)
            json_file.close()
    test_dataframe = pd.json_normalize(test_results)
    return test_dataframe


def generateReportForTestCase(test_case_name, test_dataframe, csv_dir):
    overall_duration = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.duration_ns', aggfunc='median')
    columnList = list(overall_duration.columns)
    if "sj" in columnList:
        for column in columnList:
            overall_duration[column + "-sj-rel"] = (overall_duration["sj"] - overall_duration[column]) / overall_duration["sj"]
    if "standard_merge" in columnList:
        for column in columnList:
            overall_duration[column + "-sm-rel"] = (overall_duration["standard_merge"] - overall_duration[column]) / overall_duration["standard_merge"]
    overall_duration.to_csv(os.path.join(csv_dir, 'duration_sec.csv'))
    inner_index_disk_fetch = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.inner_disk_fetch', aggfunc='median')
    inner_index_disk_fetch.to_csv(os.path.join(csv_dir, 'inner_disk_fetch.csv'))
    if 'result.inner_total_bytes_fetched' in test_dataframe.columns:
        inner_index_total_disk_fetch = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.inner_total_bytes_fetched', aggfunc='median')
        inner_index_total_disk_fetch.to_csv(os.path.join(csv_dir, 'inner_total_disk_fetch.csv'))
    getInnerIndexBuildDurationByEpsilonForTestCase(test_dataframe).to_csv(os.path.join(csv_dir, 'index_build_duration_by_epsilon.csv'))
    getInnerIndexSizeByEpsilonForTestCase(test_dataframe).to_csv(os.path.join(csv_dir, 'index_build_size_by_epsilon.csv'))

    inner_index_build_duration = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.inner_index_build_duration_ns', aggfunc='median')
    data = {}
    data['indexes'] = []
    data['build_duration'] = []
    for index in inner_index_build_duration:
        data['indexes'].append(index)
        data['build_duration'].append(inner_index_build_duration[index].mean())
    inner_index_build_duration = pd.DataFrame(data=data)
    inner_index_build_duration.to_csv(os.path.join(csv_dir, 'inner_index_build_duration_ns.csv'))

    result_checksum = (test_dataframe[['spec.common_key', 'spec.algo_name', 'result.checksum']].sort_values(by=['spec.common_key', 'spec.algo_name'])) #.loc[test_dataframe['spec.common_key']=='10'])
    test_case_ok = True
    for common_key in sorted(test_dataframe['spec.common_key'].unique()):
        checksums = result_checksum.loc[result_checksum['spec.common_key'] == common_key]
        unique_checksums = checksums['result.checksum'].unique()
        if (len(unique_checksums) != 1):
            test_case_ok = False
            #print(f"common_key: {common_key} checksum: {unique_checksums} OK")
        else:
            pass
            #print(f"common_key: {common_key} checksums don't match")
    print(test_case_name, "\t\t[Test Pass: ", test_case_ok, "]")

def copyResultsToPaper(name, test_cases):
    os.makedirs(os.path.join(dstDir, name), exist_ok=True)
    for test_case in test_cases:
        exp_name = os.path.basename(test_cases[test_case]['dir'])
        print(exp_name)
        os.makedirs(os.path.join(dstDir, name, exp_name), exist_ok=True)
        shutil.copytree(os.path.join(test_cases[test_case]['dir'], 'csv'), os.path.join(dstDir, name, exp_name), dirs_exist_ok=True)

srcDir = './sponge'
datasets = ["udense", "usparse", "normal", "lognormal", "fb", "wiki", "books"]
ops = ["join"]
threads = ["1", "4"]# "16", "32"]

for op in ops:
    for thread in threads:
        tests = {}
        for dataset in datasets:
            test_case_dir = os.path.join(srcDir, "_".join([op, dataset, thread]))
            tests[dataset] = {"dir": test_case_dir}
        test_set_name = "_".join([op, thread])
        generateReport(test_set_name, tests)
        copyResultsToPaper(test_set_name, tests)
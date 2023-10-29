#!/usr/bin/python3
import os
import json
import pandas as pd
import shutil

dstDir = '../LearnedMergerPaper/experiment'

small_join = {
    "URDense" : {
        "dir": "./sponge/uniform_dense_join_small_1"
    },
    "URSparse" : {
        "dir": "./sponge/uniform_sparse_join_small_1"
    },
    "lognormal" : {
        "dir": "./sponge/lognormal_join_small_1"
    },
    "normal": {
        "dir": "./sponge/normal_join_small_1"
    },
    "fb": {
        "dir": "./sponge/fb_join_small_1"
    },
    "osm": {
        "dir": "./sponge/osm_join_small_1"
    },
    "wiki": {
        "dir": "./sponge/wiki_join_small_1"
    },
    "books": {
        "dir": "./sponge/books_join_small_1"
    }
}

small_merge_test_cases = {
    "URDense" : {
        "dir": "./sponge/uniform_dense_merge_small_1"
    },
    "URSparse" : {
        "dir": "./sponge/uniform_sparse_merge_small_1"
    },
    "lognormal" : {
        "dir": "./sponge/lognormal_merge_small_1"
    },
    "normal": {
        "dir": "./sponge/normal_merge_small_1"
    },
    "fb": {
        "dir": "./sponge/fb_merge_small_1"
    },
    "osm": {
        "dir": "./sponge/osm_merge_small_1"
    },
    "wiki": {
        "dir": "./sponge/wiki_merge_small_1"
    },
    "books": {
        "dir": "./sponge/books_merge_small_1"
    }
}


def generateReport(name, test_cases):
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
    pd.DataFrame.from_records(inner_index_size, index=test_cases.keys()).to_csv(os.path.join(dstDir, name + '_innerIndexSize.csv'))
    pd.DataFrame.from_records(inner_index_build_duration, index = test_cases.keys()).to_csv(os.path.join(dstDir, name + '_innerIndexBuildDuration.csv'))
    
def getInnerIndexSizeForTestCase(test_dataframe):
    inner_index_size = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.inner_index_size', aggfunc='median')
    data = {}
    for index in inner_index_size:
        data[index] = inner_index_size[index].mean()
    return data

def getInnerIndexBuildDurationForTestCase(test_dataframe):
    inner_index_build_duration = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.inner_index_build_duration_ns', aggfunc='median')
    data = {}
    for index in inner_index_build_duration:
        data[index] = inner_index_build_duration[index].mean()
    return data

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
    overall_duration.to_csv(os.path.join(csv_dir, 'duration_sec.csv'))


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

def copyCsvToPaper(name, test_cases):
    for test_case in test_cases:
        exp_name = os.path.basename(test_cases[test_case]['dir'])
        print(exp_name)
        os.makedirs(os.path.join(dstDir, exp_name), exist_ok=True)
        shutil.copytree(os.path.join(test_cases[test_case]['dir'], 'csv'), os.path.join(dstDir, exp_name), dirs_exist_ok=True)

generateReport('small_join', small_join)
generateReport('small_merge', small_merge_test_cases)

copyCsvToPaper('small_join_synthetic', small_join)
copyCsvToPaper('small_merge', small_merge_test_cases)
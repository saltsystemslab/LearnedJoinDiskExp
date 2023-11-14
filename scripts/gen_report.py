#!/usr/bin/python3
import os
import json
import pandas as pd
import shutil

dstDir = '../LearnedMergerPaper/experiment'

small_join_test_cases = {
    "URDense" : {
        "dir": "./sponge/uniform_dense_small_join_1"
    },
    "URSparse" : {
        "dir": "./sponge/uniform_sparse_small_join_1"
    },
    "lognormal" : {
        "dir": "./sponge/lognormal_small_join_1"
    },
    "normal": {
        "dir": "./sponge/normal_small_join_1"
    },
    "fb": {
        "dir": "./sponge/fb_small_join_1"
    },
    "wiki": {
        "dir": "./sponge/wiki_small_join_1"
    }
    # Disabled for now, these are large datasets.
    #"books": {
    #    "dir": "./sponge/books_small_join_small_1"
    #}
    #"osm": {
    #    "dir": "./sponge/osm_small_join_small_1"
    #},
}

small_merge_test_cases = {
    "URDense" : {
        "dir": "./sponge/uniform_dense_small_merge_1"
    },
    "URSparse" : {
        "dir": "./sponge/uniform_sparse_small_merge_1"
    },
    "lognormal" : {
        "dir": "./sponge/lognormal_small_merge_1"
    },
    "normal": {
        "dir": "./sponge/normal_small_merge_1"
    },
    "fb": {
        "dir": "./sponge/fb_small_merge_1"
    },
    "wiki": {
        "dir": "./sponge/wiki_small_merge_1"
    }
    #"books": {
    #    "dir": "./sponge/books_small_merge_1"
    #},
    #"osm": {
    #    "dir": "./sponge/osm_small_merge_1"
    #}
}


join_test_cases = {
    "URDense" : {
        "dir": "./sponge/uniform_dense_join_1"
    },
    "URSparse" : {
        "dir": "./sponge/uniform_sparse_join_1"
    },
    "lognormal" : {
        "dir": "./sponge/lognormal_join_1"
    },
    "normal": {
        "dir": "./sponge/normal_join_1"
    },
    "fb": {
        "dir": "./sponge/fb_join_1"
    },
    "wiki": {
        "dir": "./sponge/wiki_join_1"
    }
    # Disabled for now, these are large datasets.
    #"books": {
    #    "dir": "./sponge/books_join_small_1"
    #}
    #"osm": {
    #    "dir": "./sponge/osm_join_small_1"
    #},
}

join_ratio10= {
    "fb": {
        "dir": "./sponge/fb_join-ratio10_1"
    },
}

join_search = {
    "fb": {
        "dir": "./sponge/fb_search_1"
    },
}

merge_ratio10= {
    "fb": {
        "dir": "./sponge/fb_merge-ratio10_1"
    },
}

merge_test_cases = {
    "URDense" : {
        "dir": "./sponge/uniform_dense_merge_1"
    },
    "URSparse" : {
        "dir": "./sponge/uniform_sparse_merge_1"
    },
    "lognormal" : {
        "dir": "./sponge/lognormal_merge_1"
    },
    "normal": {
        "dir": "./sponge/normal_merge_1"
    },
    "fb": {
        "dir": "./sponge/fb_merge_1"
    },
    "wiki": {
        "dir": "./sponge/wiki_merge_1"
    }
    #"books": {
    #    "dir": "./sponge/books_merge_1"
    #},
    #"osm": {
    #    "dir": "./sponge/osm_merge_1"
    #}
}

join_4_test_cases = {
    "URDense" : {
        "dir": "./sponge/uniform_dense_join_4"
    },
    "URSparse" : {
        "dir": "./sponge/uniform_sparse_join_4"
    },
    "lognormal" : {
        "dir": "./sponge/lognormal_join_4"
    },
    "normal": {
        "dir": "./sponge/normal_join_4"
    },
    "fb": {
        "dir": "./sponge/fb_join_4"
    },
    "wiki": {
        "dir": "./sponge/wiki_join_4"
    }
    # Disabled for now, these are large datasets.
    #"books": {
    #    "dir": "./sponge/books_join_small_4"
    #}
    #"osm": {
    #    "dir": "./sponge/osm_join_small_4"
    #},
}

merge_4_test_cases = {
    "URDense" : {
        "dir": "./sponge/uniform_dense_merge_4"
    },
    "URSparse" : {
        "dir": "./sponge/uniform_sparse_merge_4"
    },
    "lognormal" : {
        "dir": "./sponge/lognormal_merge_4"
    },
    "normal": {
        "dir": "./sponge/normal_merge_4"
    },
    "fb": {
        "dir": "./sponge/fb_merge_4"
    },
    "wiki": {
        "dir": "./sponge/wiki_merge_4"
    }
    #"books": {
    #    "dir": "./sponge/books_merge_4"
    #},
    #"osm": {
    #    "dir": "./sponge/osm_merge_4"
    #}
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
    pd.DataFrame.from_records(inner_index_size, index=test_cases.keys()).to_csv(os.path.join(dstDir, name,  'innerIndexSize.csv'))
    pd.DataFrame.from_records(inner_index_build_duration, index = test_cases.keys()).to_csv(os.path.join(dstDir, name, 'innerIndexBuildDuration.csv'))
    
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
    inner_index_disk_fetch = test_dataframe.pivot_table(index='spec.common_key', columns='spec.algo_name', values='result.inner_disk_fetch', aggfunc='median')
    inner_index_disk_fetch.to_csv(os.path.join(csv_dir, 'inner_disk_fetch.csv'))

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

#generateReport('join_optimalIo', small_join)
#copyResultsToPaper('join_optimalIo', small_join)

#generateReport('merge', merge_test_cases)
#copyResultsToPaper('merge', merge_test_cases)

# What is the difference between generateReport and copyResultsToPaper?

generateReport('join_4threads', join_4_test_cases)
copyResultsToPaper('join_4threads', join_4_test_cases)

generateReport('merge_4threads', merge_4_test_cases)
copyResultsToPaper('merge_4threads', merge_4_test_cases)

generateReport('join_ratio10', join_ratio10)
copyResultsToPaper('join_ratio10', join_ratio10)

generateReport('merge_ratio10', merge_ratio10)
copyResultsToPaper('merge_ratio10', merge_ratio10)

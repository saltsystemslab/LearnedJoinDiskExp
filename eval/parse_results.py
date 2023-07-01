import pandas as pd
from dataclasses import dataclass,asdict
from dataclass_builder import (dataclass_builder, build)

@dataclass
class Iterator:
    num_items: int = 0
    creation_sec: float = 0.0
    model_size_bytes: int = 0

@dataclass
class TestRun:
    iter_0: Iterator = Iterator(0, 0, 0)
    iter_1: Iterator = Iterator(0, 0, 0)
    merge_duration_sec: float = 0.0
    status: str = 'Failed'

    merge_mode: str = 'no_op'
    use_disk: bool = False
    key_size_bytes: int = 0
    num_threads: int = 1
    ratio: float = 0.0
    plr_error: int = 0


def benchmark_output_as_dataframe(benchmark_output_file_path):
    '''Return a dataframe containing benchmark results'''
    file = open(benchmark_output_file_path)
    lines = file.readlines()
    test_runs = parse_lines_into_test_runs(lines)
    df = pd.json_normalize(test_runs)
    df['iter_0.item_total_size_bytes'] = df['iter_0.num_items'] * df['key_size_bytes']
    df['iter_1.item_total_size_bytes'] = df['iter_1.num_items'] * df['key_size_bytes']
    return df


def parse_lines_into_test_runs(lines):
    '''Returns a list of TestRuns from lines'''
    # The output of src/benchmark.cpp is not cleanly structured.
    # Ideally we'd like to use something like JSON for this.
    # Until then, this parsing logic is closely tied to the output of
    # src/benchmark.cpp, is brittle.
    runs = []
    run_lines = []
    for l in lines:
        if l.startswith('run'): # Ignore this line
            continue 
        if l.startswith('merge_mode:'):
            runs.append(parse_lines_into_test_run(run_lines))
            run_lines = [l]
        else:
            run_lines.append(l)

    # Don't forget the last set of lines to parse!
    runs.append(parse_lines_into_test_run(run_lines))
    return runs

def parse_lines_into_test_run(lines):
    TestRunBuilder = dataclass_builder(TestRun)
    IteratorBuilder = dataclass_builder(Iterator)
    run_builder = TestRunBuilder()
    iter_0_builder = IteratorBuilder()
    iter_1_builder = IteratorBuilder()
    for l in lines:
        if l.startswith('merge_mode:'):
            run_builder.merge_mode = l.split()[1]
        if l.startswith("Merge duration"):
            run_builder.merge_duration_sec = parse_merge_duration(l)
        if l.startswith("Ok!"):
            run_builder.status = 'Success'
        if l.startswith("List Sizes: "):
            iter_0_builder.num_items = parse_list_size(l)[0]
            iter_1_builder.num_items = parse_list_size(l)[1]
            run_builder.ratio = iter_1_builder.num_items / iter_0_builder.num_items
        if l.startswith("Num Threads"):
            run_builder.num_threads = parse_num_threads(l)
        if l.startswith('Use Disk: '):
            run_builder.use_disk = parse_is_disk(l)
        if l.startswith('PLR_Error: '):
            run_builder.plr_error = parse_plr_error(l)
        if l.startswith('Key Bytes: '):
            run_builder.key_size_bytes = parse_key_bytes(l)
        if l.startswith("Iterator 0 creation"):
            iter_0_builder.creation_sec = parse_iter_creation_duration(l)
        if l.startswith("Iterator 1 creation"):
            iter_1_builder.creation_sec = parse_iter_creation_duration(l)
        if l.startswith("Iterator 0 model size"):
            iter_0_builder.model_size_bytes = parse_iter_model_size(l)
        if l.startswith("Iterator 1 model size"):
            iter_1_builder.model_size_bytes = parse_iter_model_size(l)
    run_builder.iter_0 = iter_0_builder.build()
    run_builder.iter_1 = iter_1_builder.build()
    return asdict(run_builder.build())


"Parse 'List Sizes: %d %d'"
def parse_list_size(l):
    tokens = l.split()
    list_size_1 = int(tokens[2].strip())
    list_size_2 = int(tokens[3].strip())
    return (list_size_1, list_size_2)

"Parse 'Use Disk: %d'"
def parse_is_disk(l):
    tokens = l.split()
    if tokens[2] == "0":
        return False
    else:
        return True

"Parse 'PLR_Error: %d'"
def parse_plr_error(l):
    tokens = l.split()
    return int(tokens[1].strip())

"Parse 'Key Bytes: %d'"
def parse_key_bytes(l):
    tokens = l.split()
    return int(tokens[2].strip())

"Parse 'Num Threads: %d'"
def parse_num_threads(l):
    tokens = l.split()
    return int(tokens[2].strip())

"Parse 'merge duration: %d'"
def parse_merge_duration(l):
    tokens = l.split()
    return float(tokens[2].strip())

"Parse 'Iterator x creation duration time: %d sec'"
def parse_iter_creation_duration(l):
    tokens = l.split()
    return float(tokens[5].strip())

"Parse 'Iterator x model size bytes: %d'"
def parse_iter_model_size(l):
    tokens = l.split()
    return int(tokens[5].strip())

df = benchmark_output_as_dataframe('run_10M_3.txt')
print(df)

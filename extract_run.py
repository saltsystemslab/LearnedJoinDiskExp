#!/usr/bin/python

import pprint
import pandas as pd

pp = pprint.PrettyPrinter(indent=2)
runs = []

merge_modes = [
    "Parallelized Learned Merge", 
    "Parallelized Standard Merge",
    "Learned Merge",
    "Standard Merge",
]

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

file = open('out.txt', 'r')
lines = file.readlines()
run = {}
for l in lines:
    if l.startswith("List Sizes: "):
        if run:
            runs.append(run)
            run = {}
            run['num_threads'] = 1
        run['list_sizes'] = parse_list_size(l)
    if l.startswith("Use Disk: "):
        run['use_disk'] = parse_is_disk(l)
    if l.startswith("PLR_Error"):
        run['plr_error_bound'] = parse_plr_error(l)
    if l.startswith("Key Bytes"):
        run['key_bytes'] = parse_key_bytes(l)
    if l.startswith("Num Threads"):
        run['num_threads'] = parse_num_threads(l)
    for m in merge_modes:
        if l.startswith(m) and 'merge_mode' not in run.keys():
            run['merge_mode'] = m
    if l.startswith("Merge duration"):
        run['merge_duration_sec'] = parse_merge_duration(l)
    if l.startswith("Iterator 0 creation"):
        run['iter_0_creation_sec'] = parse_iter_creation_duration(l)
    if l.startswith("Iterator 1 creation"):
        run['iter_1_creation_sec'] = parse_iter_creation_duration(l)
    if l.startswith("Iterator 0 model size"):
        run['iter_0_model_size_bytes'] = parse_iter_model_size(l)
    if l.startswith("Iterator 1 model size"):
        run['iter_1_model_size_bytes'] = parse_iter_model_size(l)
    if l.startswith("Merge duration"):
        run['merge_duration_sec'] = parse_merge_duration(l)
    

pp.pprint(runs)
print(pd.DataFrame.from_dict(runs))

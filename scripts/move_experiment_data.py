#!/usr/bin/python3

import os
import shutil

srcDir = 'sponge'
dstDir = '../LearnedMergerPaper/experiment'

expList = [
    'ur_uint64_join',
    'ur_uint64_merge_1'
]

for exp in os.listdir(srcDir):
    if exp not in expList:
        continue
    os.makedirs(os.path.join(dstDir, exp), exist_ok=True)
    shutil.copytree(os.path.join(srcDir, exp, 'csv'), os.path.join(dstDir, exp), dirs_exist_ok=True)




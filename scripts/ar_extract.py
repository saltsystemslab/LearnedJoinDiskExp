#!/usr/bin/python3
import sys
import os
import csv

tsv_dir = sys.argv[1]
outfile = sys.argv[2]

files = []
for f in os.listdir(tsv_dir):
    if f.endswith(".tsv"):
        files.append(f)

header_bytes = 16
review_id_len = 16

keys = []
num_keys = 0
for filename in files:
    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        for row in reader:
            review_id = row['review_id']
            review_id_bytes = bytearray(review_id.encode('ascii'))
            assert(len(review_id_bytes) < review_id_len)
            while len(review_id_bytes) < review_id_len:
                review_id_bytes.insert(0, 0)
            keys.append(bytes(review_id_bytes))

with open(outfile, "wb") as outfile:
    outfile.seek(header_bytes)
    keys.sort()
    for key in keys:
        outfile.write(key)

    outfile.seek(0)
    num_keys = len(keys)
    num_keys_as_bytes = bytearray(num_keys.to_bytes(8, byteorder='little'))
    key_len_as_bytes = bytearray(review_id_len.to_bytes(2, byteorder='little'))
    padding  = bytearray(int("0").to_bytes(6, byteorder='little'))
    print(num_keys)
    outfile.write(num_keys_as_bytes)
    outfile.write(key_len_as_bytes)


        


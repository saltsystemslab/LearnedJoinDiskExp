#!/usr/bin/python3

import sys

d = {}
mx = 0

with open(sys.argv[1], "rb") as f:
    f.read(16)
    for i in range(0, 4000000):
        k = f.read(8)
        v = f.read(8)
        if k not in d:
            d[k] = 0
        d[k] += 1
        mx = max(d[k], mx)
for k in d:
    if d[k] > 100:
        print(k, d[k])

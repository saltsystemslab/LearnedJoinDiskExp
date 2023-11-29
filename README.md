
```

#Index Build Stats and Duration

```
$python3 benchmark/run_indexes.py # (1 hr on patagonia).
# Use scripts/index_report.ipynb
```
# To run all tests
# Back up sponge rename to a new directory.
$python3 benchmark/run_all.py # (Make sure to check the configuration)


# To copy CSV files change directory in script/gen_report.py
./benchmark/gen_report.py

```

To debug a single test case use ./benchmark/report.ipynb.
To get grouped results use ./benchmark/mergejoin_report.ipynb
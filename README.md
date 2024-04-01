## Run all experiments

Setup Input Datasets, see below.

```
./experiments/join_all/run.py --sosd_data_dir=<PATH_TO_DIR> --use_numactl

```

## Join & Merge Runner

## Join Merge Binary

```bash
mkdir build
cd build
cmake ../ -DCMAKE_BUILD_TYPE=Release
cmake --build .
./benchmark_runner config.json
```

### Example config.json

You don't have to worry about writing these config jsons, use the script to trigger the binary.
But here's how one looks anyways.

```json
{
    "algo": "inlj",
    "algo_name": "inlj_btree256",
    "check_checksum": true,
    "common_key": 10,
    "index": {
        "epsilon": 256,
        "leaf_size_in_pages": 1,
        "search": "binary",
        "type": "btree"
    },
    "inner_table": "sponge/join_all/books_1/inputs/inner",
    "key_size": 8,
    "key_type": "uint64",
    "load_sstable_in_mem": false,
    "name": "inlj_btree25610run_2",
    "num_threads": 1,
    "outer_table": "sponge/join_all/books_1/inputs/input10",
    "result_path": "sponge/join_all/books_1/outputs/inlj_btree256_run_2_ratio_10",
    "value_size": 8,
    "write_result_to_disk": true
}
```

## Join & Merge Script Runner

```bash
python3 ./scripts/benchmark.py --spec=benchmark/sosd_join.jsonnet --threads=3 --repeat=3 --test_name=join_test --sosd_source=join_inputdataset --sosd_num_keys=1000000000 --clear_inputs=False
```



## Input Datasets

### SOSD

```bash
sudo apt -y update
sudo apt -y install zstd python3-pip m4 cmake clang libboost-all-dev
pip3 install --user numpy scipy
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

git clone git@github.com:learnedsystems/sosd
cd sosd
./scripts/download.sh
```

### String keys

TODO



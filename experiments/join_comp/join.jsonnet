local key_size = 8;
local value_size = 8;
local key_template = {
    "key_type": "uint64", // Can also be str
    "key_size": key_size,
    "value_size": value_size
};
local input_template = key_template + {
    "algo": "create_input",
    "method": "sosd",
    "write_result_to_disk": true, // Can be false.
    "source": std.extVar("TEST_DATASET_SOURCE")
};
local test_output_dir = std.extVar("TEST_OUTPUT_DIR");
local test_input_dir = std.extVar("TEST_INPUT_DIR");
local repeats = std.parseInt(std.extVar("TEST_REPEAT"));
local num_threads = std.parseInt(std.extVar("TEST_NUM_THREADS"));
local num_keys_in_inner = std.parseInt(std.extVar("TEST_DATASET_SIZE"));

local ratios = [1, 10, 100, 1000];
{
    inputs : 
        [input_template + {
            local name = "inner",
            "name": name,
            "num_keys": num_keys_in_inner,
            "result_path": test_input_dir + "/inner",
            "create_indexes": true,
        }] +
        [
            input_template + {
            local name = "input" + i,
            "num_keys": std.ceil(num_keys_in_inner/i),
            "name": name,
            "result_path": test_input_dir + "/" + name,
            "create_indexes": false,
            }  for i in ratios
        ],
    tests: [
        key_template + algo + {
            "name": algo["algo_name"] + i + "run_" + r,
            "common_key": i,
            "num_threads" : num_threads,
            "inner_table" : test_input_dir + "/inner",
            "outer_table" : test_input_dir + "/input" + i,
            "result_path": test_output_dir + "/" + algo["algo_name"] + "_" + "run_" + r + "_ratio_"+ i,
            "load_sstable_in_mem": false,
            "write_result_to_disk": true,
            "check_checksum": if std.extVar('TEST_CHECK_CHECKSUM') == "True" then true else false,
        }   
        for r in std.range(0, repeats)
        for i in ratios
        for algo in [
            {
                "algo_name": "sj",
                "algo": "sort_join",
            }, 
            {
                "algo_name": "hj",
                "algo": "hash_join",
            }, 
            {
                "algo_name": "inljBtree256",
                "algo": "inlj",
                "index": {
                    "type": "btree",
                    "search": "binary",
                    "leaf_size_in_pages": 1,
                },
            },
            {
                "algo_name": "inljBtree1024",
                "algo": "inlj",
                "index": {
                    "type": "btree",
                    "search": "binary",
                    "leaf_size_in_pages": 4,
                },
            },
            {
                "algo_name": "inljBtree4096",
                "algo": "inlj",
                "index": {
                    "type": "btree",
                    "search": "binary",
                    "leaf_size_in_pages": 8,
                },
            },
            {
                "algo_name": "inljPgm256",
                "algo": "inlj",
                "index": {
                    "type": "pgm256",
                    "search": "binary",
                },
            },
            {
                "algo_name": "inljPgm256",
                "algo": "inlj",
                "index": {
                    "type": "pgm256",
                    "search": "binary",
                },
            },
            {
                "algo_name": "inljPgm256",
                "algo": "inlj",
                "index": {
                    "type": "pgm256",
                    "search": "binary",
                },
            },
            {
                "algo_name": "flatpgm2048",
                "algo": "lsj",
                "index": {
                    "type": "flatpgm2048",
                    "search": "binary",
                },
            },
            {
                "algo_name": "flatpgm256",
                "algo": "lsj",
                "index": {
                    "type": "flatpgm256",
                    "search": "binary",
                },
            },
            {
                "algo_name": "btree2048",
                "algo": "lsj",
                "index": {
                    "type": "btree",
                    "search": "binary",
                    "leaf_size_in_pages": 8,
                },
            },
        ]
    ]
}

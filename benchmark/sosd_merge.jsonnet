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
local num_common_keys = 10000;

local max_ratio = 100;
local points = 10;
local step = max_ratio / points;
local ratios = std.map(function(x) std.ceil(step * x), std.range(1, points));

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
        }   
        for r in std.range(0, repeats)
        for i in ratios
        for algo in [
            {
                "algo_name": "standard_merge",
                "algo": "standard_merge",
            }, 
            {
                "algo": "learned_merge",
                "index": {
                    "type": "pgm256",
                    "search": "binary",
                },
                "algo_name": "pgm256",
            },
            {
                "algo": "learned_merge",
                "index": {
                    "type": "pgm1024",
                    "search": "binary",
                },
                "algo_name": "pgm1024",
            },
            {
                "algo": "learned_merge",
                "index": {
                    "type": "pgm2048",
                    "search": "binary",
                },
                "algo_name": "pgm2048",
            },
            {
                "algo": "learned_merge",
                "index": {
                    "type": "btree",
                    "leaf_size_in_pages": 1,
                    "search": "binary",
                },
                "algo_name": "btree256",
            },
            {
                "algo": "learned_merge",
                "index": {
                    "type": "btree",
                    "leaf_size_in_pages": 4,
                    "search": "binary",
                },
                "algo_name": "btree1024",
            },
            {
                "algo": "learned_merge",
                "index": {
                    "type": "btree",
                    "leaf_size_in_pages": 8,
                    "search": "binary",
                },
                "algo_name": "btree2048",
            },
        ]
    ]
}

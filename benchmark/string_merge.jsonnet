local key_size = 16;
local value_size = 16;
local key_template = {
    "key_type": "str", // Can also be str
    "key_size": key_size,
    "value_size": value_size
};
local input_template = key_template + {
    "algo": "create_input",
    "method": "string",
    "write_result_to_disk": true, // Can be false.
};
local test_output_dir = std.extVar("TEST_OUTPUT_DIR");
local test_input_dir = std.extVar("TEST_INPUT_DIR");
local repeats = std.parseInt(std.extVar("TEST_REPEAT"));
local num_threads = std.parseInt(std.extVar("TEST_NUM_THREADS"));
local num_keys_in_inner = std.parseInt(std.extVar("TEST_DATASET_SIZE"));
local num_common_keys = 10000;

local ratios = [1, 5, 10, 50, 100];

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
            "source": test_input_dir + "/inner",
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
                    "leaf_size_in_pages": 2,
                    "search": "binary",
                },
                "algo_name": "btree256",
            },
            {
                "algo": "learned_merge",
                "index": {
                    "type": "btree",
                    "leaf_size_in_pages": 8,
                    "search": "binary",
                },
                "algo_name": "btree1024",
            },
            {
                "algo": "learned_merge",
                "index": {
                    "type": "btree",
                    "leaf_size_in_pages": 16,
                    "search": "binary",
                },
                "algo_name": "btree2048",
            },
        ]
    ]
}
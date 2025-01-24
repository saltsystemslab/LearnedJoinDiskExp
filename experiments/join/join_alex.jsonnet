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
local dataset_name = std.extVar("TEST_DATASET_NAME");
local test_output_dir = std.extVar("TEST_OUTPUT_DIR");
local test_input_dir = std.extVar("TEST_INPUT_DIR");
local repeats = std.parseInt(std.extVar("TEST_REPEAT"));
local num_threads = std.parseInt(std.extVar("TEST_NUM_THREADS"));
local num_keys_in_inner = std.parseInt(std.extVar("TEST_DATASET_SIZE"));
local ratios = std.parseJson(std.extVar("TEST_RATIOS"));

local algos = [
    {
        "algo_name": "alex_inlj",
        "algo": "alex_inlj",
        "index": {
            "type": "alex",
            "search": "binary",
            "epsilon": 0,
        }
    }
];


{
    inputs : 
        [input_template + {
            local name = "inner",
            "name": name,
            "num_keys": num_keys_in_inner,
            "result_path": test_input_dir + "/inner",
            "create_indexes": true,
            "fraction_of_keys": 1,
            "dataset": dataset_name,
        }] +
        [
            input_template + {
            local name = "input" + i,
            "num_keys": std.ceil(num_keys_in_inner/i),
            "fraction_of_keys": i,
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
        for algo in algos
    ]
}

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

local max_ratio = 100;
local points = 10;
local step = max_ratio / points;
local ratios = []; //std.map(function(x) std.ceil(step * x), std.range(1, points)); // We just want to create the inner index.

{
    inputs : 
        [input_template + {
            local name = "inner",
            "name": name,
            "num_keys": num_keys_in_inner,
            "result_path": test_input_dir + "/inner",
            "create_indexes": true,
        }],
    tests: []
}

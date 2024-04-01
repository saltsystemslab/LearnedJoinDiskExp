local input_template = {
    "key_type": "uint64", // Can also be str
    "key_size": 8,
    "value_size": 8, 
    "algo": "create_input",
    "method": "sosd",
    "write_result_to_disk": true, // Can be false.
    "source": std.extVar("TEST_DATASET_SOURCE")
};

local test_input_data_dir = std.extVar("TEST_INPUT_DIR");
local num_keys_in_inner = std.parseInt(std.extVar("TEST_DATASET_SIZE"));
{
    inputs : 
        [input_template + {
            local name = "inner",
            "name": name,
            "num_keys": num_keys_in_inner,
            "result_path": test_input_data_dir + "/inner",
            "create_indexes": true,
        }],
    tests: []
}

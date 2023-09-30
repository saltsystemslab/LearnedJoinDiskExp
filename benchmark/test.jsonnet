local key_size = 8;
local value_size = 8;
local key_template = {
    "key_type": "uint64", // Can also be str
    "key_size": key_size,
    "value_size": value_size
};
local input_template = key_template + {
    "algo": "create_input",
    "method": "uniform_dist",
    "write_result_to_disk": true, // Can be false.
};
local test_output_dir = std.extVar("TEST_OUTPUT_DIR");
local test_input_dir = std.extVar("TEST_INPUT_DIR");
{
    inputs : [
        input_template + {
            local name = "common",
            "name": name,
            "num_keys": 100,
            "result_path": test_input_dir + "/" + name,
        },
        input_template + {
            local name = "input1",
            "name": name,
            "num_keys": 10000,
            "common_keys_file": test_input_dir + "/" + "common",
            "result_path": test_input_dir + "/" + name,
        },
        input_template + {
            local name = "input2",
            "name": name,
            "num_keys": 100000,
            "common_keys_file": test_input_dir + "/" + "common",
            "result_path": test_input_dir + "/" + name,
        },
    ],
    tests: [
        key_template + {
            "name": "sort_join",
            "algo": "sort_join",
            "inner_table": test_input_dir + "/input1",
            "outer_table": test_input_dir + "/input2",
            "num_threads": 1,
            "write_result_to_disk": true,
            "result_path": test_output_dir + "/sort_join",
            "load_sstable_in_mem": false,
        }
    ]
}
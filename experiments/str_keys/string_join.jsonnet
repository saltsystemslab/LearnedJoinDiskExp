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

local all_index_names = [
    "pgm256", "flatpgm256", "sampledflatpgm256", 
    "pgm1024", "flatpgm1024", "sampledflatpgm1024", 
    "pgm4096", "flatpgm4096", "sampledflatpgm4096", 
    "btree256", "btree1024", "btree4096"
];
local index_names = [
    "flatpgm256", 
    "flatpgm1024", 
    "flatpgm4096", 
    "btree256", "btree1024", "btree4096"
];

local indexes = {
    "pgm256": {
        "type": "pgm256",
        "search": "binary",
        "epsilon": 256
    },
    "flatpgm256": {
        "type": "flatpgm256",
        "search": "binary",
        "epsilon": 256
    },
    "sampledflatpgm256": {
        "type": "sampledflatpgm256",
        "search": "binary",
        "epsilon": 256
    },

    "pgm1024": {
        "type": "pgm1024",
        "search": "binary",
        "epsilon": 1024
    },
    "flatpgm1024": {
        "type": "flatpgm1024",
        "search": "binary",
        "epsilon": 1024
    },
    "sampledflatpgm1024": {
        "type": "sampledflatpgm1024",
        "search": "binary",
        "epsilon": 1024
    },

    "pgm4096": {
        "type": "pgm4096",
        "search": "binary",
        "epsilon": 4096
    },
    "flatpgm4096": {
        "type": "flatpgm4096",
        "search": "binary",
        "epsilon": 4096
    },
    "sampledflatpgm4096": {
        "type": "sampledflatpgm4096",
        "search": "binary",
        "epsilon": 4096
    },


    "btree256": {
        "type": "btree",
        "search": "binary",
        "leaf_size_in_pages": 2,
        "epsilon": 256,
    },
    "btree1024": {
        "type": "btree",
        "search": "binary",
        "leaf_size_in_pages": 16,
        "epsilon": 1024,
    },
    "btree4096": {
        "type": "btree",
        "search": "binary",
        "leaf_size_in_pages": 32,
        "epsilon": 4096,
    }
};

local algos = [
    {
        "algo_name": join_algo + "_" + idx,
        "algo": join_algo,
        "index": indexes[idx]
    }
    for idx in index_names
    for join_algo in ["lsj", "inlj"]
] + [
    {
        "algo_name": "sort_join",
        "algo": "sort_join"
    },
    {
        "algo_name": "hash_join",
        "algo": "hash_join"
    },
];

local max_ratio = 100;
local points = 10;
local step = max_ratio / points;
local ratios = [1, 10, 100, 1000];
{
    inputs : 
        [input_template + {
            local name = "inner",
            "name": name,
            "num_keys": num_keys_in_inner,
            "result_path": test_input_dir + "/inner",
            "fraction_of_keys": 1,
            "create_indexes": true,
        }] +
        [
            input_template + {
            local name = "input" + i,
            "num_keys": std.ceil(num_keys_in_inner/i),
            "name": name,
            "result_path": test_input_dir + "/" + name,
            "fraction_of_keys": 1,
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
            "check_checksum": false,
        }   
        for r in std.range(0, repeats)
        for i in ratios
        for algo in algos   
    ]
}

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
local index_names = std.parseJson(std.extVar("TEST_INDEXES"));
local non_indexed_joins = std.parseJson(std.extVar("TEST_NON_INDEXED_JOINS"));
local indexed_joins = std.parseJson(std.extVar("TEST_INDEXED_JOINS"));

local all_index_names = [
    "pgm256", "flatpgm256", "sampledflatpgm256", 
    "pgm1024", "flatpgm1024", "sampledflatpgm1024", 
    "pgm4096", "flatpgm4096", "sampledflatpgm4096", 
    "btree256", "btree1024", "btree4096", 
    "radixspline256", "radixspline1024", "radixspline4096",
    "rmi",
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
        "leaf_size_in_pages": 1,
        "epsilon": 256,
    },
    "btree1024": {
        "type": "btree",
        "search": "binary",
        "leaf_size_in_pages": 4,
        "epsilon": 1024,
    },
    "btree4096": {
        "type": "btree",
        "search": "binary",
        "leaf_size_in_pages": 16,
        "epsilon": 4096,
    },
    "radixspline256": {
        "type": "radixspline256",
        "search": "binary",
        "epsilon": 256,
    },
    "radixspline1024": {
        "type": "radixspline1024",
        "search": "binary",
        "epsilon": 1024,
    },
    "radixspline4096": {
        "type": "radixspline4096",
        "search": "binary",
        "epsilon": 4096,
    },
    "rmi": {
        "type": "rmi",
        "search": "binary",
        "dataset": dataset_name,
    }
};

local algos = [
    {
        "algo_name": join_algo + "_" + idx,
        "algo": join_algo,
        "index": indexes[idx]
    }
    for idx in index_names
     for join_algo in indexed_joins
] + [
    {
        "algo_name": algo,
        "algo": algo,
    } 
    for algo in non_indexed_joins
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

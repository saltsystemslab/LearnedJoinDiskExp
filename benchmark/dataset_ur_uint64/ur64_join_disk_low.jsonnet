local join_algos = import '../common/join_algos.libsonnet';
local input_util = import '../common/input_util.libsonnet'; 
local key_size = 8;
local value_size = 8;
local key_type = "uint64";
local inputs = input_util.create_join_input(1000000, 1000, 100, 20);
{
    "algos":  {
        "common": {
        "key_size": key_size,
        "value_size": value_size,
        "key_type": key_type,
        "load_sstable_in_mem": false,
        "write_result_to_disk": true,
        },
        "list": join_algos['list'],
        "baseline": "sort_join",
    },
    "inputs": {
        "common": {
            "algo": "create_input",
            "key_size": key_size,
            "value_size": value_size,
            "key_type": key_type,
            "write_result_to_disk" : true,
            "method": "uniform_dist"
        },
        "list": inputs["input"],
    },
    "metrics": [
      [
         "result",
         "duration_sec"
      ],
      [
         "result",
         "inner_index_size"
      ],
      [
         "result",
         "outer_index_size"
      ]
    ],
    "tests": inputs["tests"],
    "repeat": 1,
}
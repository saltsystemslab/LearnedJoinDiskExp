local key_size = 8;
local value_size = 8;
local key_type = "uint64";
{
   "num_keys": 1000000,
   "key_size": key_size,
   "key_type": key_type,
   "value_size": value_size,
    "algos": {
      "common": {
         "key_size": key_size,
         "key_type": key_type,
         "load_sstable_in_mem": false,
         "value_size": value_size,
         "write_result_to_disk": true,
         "num_threads": 4,
      },
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
    }
}
local key_size = 16;
local value_size = 16;
local key_type = "str";
{
   "num_keys": 100000000,
   "key_size": key_size,
   "key_type": key_type,
   "value_size": value_size,
    "algos": {
      "common": {
         "key_size": key_size,
         "key_type": key_type,
         "load_sstable_in_mem": false,
         "value_size": value_size,
         "write_result_to_disk": true
      },
    },
    "inputs": {
        "common": {
            "algo": "create_input",
            "key_size": key_size,
            "value_size": value_size,
            "key_type": key_type,
            "write_result_to_disk" : true,
            "method": "from_ar",
            "source": "./datasets/ar_keys_v1_00",
        },
    }
}
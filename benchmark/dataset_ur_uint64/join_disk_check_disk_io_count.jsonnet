local input_util = import '../common/input_util.libsonnet'; 
local common = import './common.libsonnet';
local inputs = input_util.create_join_input(common["num_keys"], 1000, 100, 10);
{
   "algos": {
      "common": common["algos"]["common"] + { "num_threads": 1},
      "list": [
         {
            "algo": "inlj",
            "index": {
               "type": "pgm8"
            },
            "name": "PGM_8"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "pgm64"
            },
            "name": "PGM_64"
         },
         {
            "algo": "sort_join",
            "name": "sort_join"
         },
         {
            "algo": "inlj_btree",
            "name": "BTree"
         },
      ],
   },
    "inputs": {
        "common": common["inputs"]["common"],
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
      ],
      [
         "result",
         "inner_disk_fetch"
      ],
      [
         "result",
         "outer_disk_fetch"
      ]
    ],
    "tests": inputs["tests"],
    "repeat": 5,
}

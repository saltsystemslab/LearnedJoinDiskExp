local input_util = import '../common/input_util.libsonnet'; 
local common = import './common.libsonnet';
local inputs = input_util.create_join_input(common["num_keys"], 1000, 100, 10);
{
   "algos": {
      "common": common["algos"]["common"] + { "num_threads": 4},
      "list": [
         {
            "algo": "inlj",
            "index": {
               "type": "onelevel_pgm8"
            },
            "name": "ONELEVEL_PGM_8"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "onelevel_pgm64"
            },
            "name": "ONELEVEL_PGM_64"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "onelevel_pgm128"
            },
            "name": "ONELEVEL_PGM_128"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "onelevel_pgm256"
            },
            "name": "ONELEVEL_PGM_256"
         },
         {
            "algo": "inlj",
            "index": {
               "type": "onelevel_pgm1024"
            },
            "name": "ONELEVEL_PGM_1024"
         },
         {
            "algo": "inlj",
            "index": {
               "error_bound": 8,
               "type": "plr"
            },
            "name": "PLR_8"
         },
         {
            "algo": "inlj",
            "index": {
               "error_bound": 64,
               "type": "plr"
            },
            "name": "PLR_64"
         },
         {
            "algo": "inlj",
            "index": {
               "error_bound": 128,
               "type": "plr"
            },
            "name": "PLR_128"
         },
         {
            "algo": "inlj",
            "index": {
               "error_bound": 256,
               "type": "plr"
            },
            "name": "PLR_256"
         },
         {
            "algo": "inlj",
            "index": {
               "error_bound": 1024,
               "type": "plr"
            },
            "name": "PLR_1024"
         },
         {
            "algo": "sort_join",
            "name": "sort_join"
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
      ]
    ],
    "tests": inputs["tests"],
    "repeat": 5,
}
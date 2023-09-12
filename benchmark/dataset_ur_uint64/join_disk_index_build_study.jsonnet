local input_util = import '../common/input_util.libsonnet'; 
local common = import './common.libsonnet';
local inputs = input_util.create_join_input(common["num_keys"], 1, 100, 10);
{
   "algos": {
      "common": common["algos"]["common"] + { "num_threads": 4},
      "list": [
         {
            "algo": "index_study",
            "index": {
               "type": "onelevel_pgm8"
            },
            "name": "ONELEVEL_PGM_8"
         },
         {
            "algo": "index_study",
            "index": {
               "type": "onelevel_pgm64"
            },
            "name": "ONELEVEL_PGM_64"
         },
         {
            "algo": "index_study",
            "index": {
               "type": "onelevel_pgm128"
            },
            "name": "ONELEVEL_PGM_128"
         },
         {
            "algo": "index_study",
            "index": {
               "type": "onelevel_pgm256"
            },
            "name": "ONELEVEL_PGM_256"
         },
         {
            "algo": "index_study",
            "index": {
               "type": "onelevel_pgm1024"
            },
            "name": "ONELEVEL_PGM_1024"
         },
         {
            "algo": "index_study",
            "index": {
               "error_bound": 8,
               "type": "plr"
            },
            "name": "PLR_8"
         },
         {
            "algo": "index_study",
            "index": {
               "error_bound": 64,
               "type": "plr"
            },
            "name": "PLR_64"
         },
         {
            "algo": "index_study",
            "index": {
               "error_bound": 128,
               "type": "plr"
            },
            "name": "PLR_128"
         },
         {
            "algo": "index_study",
            "index": {
               "error_bound": 256,
               "type": "plr"
            },
            "name": "PLR_256"
         },
         {
            "algo": "index_study",
            "index": {
               "error_bound": 1024,
               "type": "plr"
            },
            "name": "PLR_1024"
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
         "inner_index_build_duration"
      ],
      [
         "result",
         "outer_index_build_duration"
      ],
    ],
    "tests": inputs["tests"],
    "repeat": 5,
}

local input_util = import '../common/input_util.libsonnet'; 
local common = import './common.libsonnet';
local inputs = input_util.create_join_input(common["num_keys"], 1000, 100, 2);
{
   "algos": {
      "common": common["algos"]["common"] + { "num_threads": 4},
      "list": [
         {
            "algo": "inlj_pgm",
            "index": {
               "type": "pgm8"
            },
            "name": "PGM_8"
         },
         {
            "algo": "inlj_pgm",
            "index": {
               "type": "pgm64"
            },
            "name": "PGM_64"
         },
         {
            "algo": "inlj_pgm",
            "index": {
               "type": "pgm128"
            },
            "name": "PGM_128"
         },
         {
            "algo": "inlj_pgm",
            "index": {
               "type": "pgm256"
            },
            "name": "PGM_256"
         },
         {
            "algo": "inlj_pgm",
            "index": {
               "type": "pgm1024"
            },
            "name": "PGM_1024"
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
         "inner_index_build_duration_sec"
      ],
      [
         "result",
         "outer_index_size"
      ],
      [
         "result",
         "outer_index_build_duration_sec"
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

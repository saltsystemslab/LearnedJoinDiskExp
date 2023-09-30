local join_algos = import '../common/join_algos.libsonnet';
local input_util = import '../common/input_util.libsonnet'; 
local common = import './common.libsonnet';
local inputs = input_util.create_all_join_input_sosd(common["num_keys"], 1000, 100, 10);
{
   "algos": {
      "common": common["algos"]["common"],
      "list": join_algos.get_join_algos(common["key_type"], common["key_size"], common["value_size"]),
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
      ],
      [
         "result",
         "inner_index_build_duration_sec"
      ],
      [
         "result",
         "outer_index_build_duration_sec"
      ],
    ],
    "tests": inputs["tests"],
    "repeat": 1,
}

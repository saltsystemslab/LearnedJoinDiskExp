local merge_algos = import '../common/merge_algos.libsonnet';
local input_util = import '../common/input_util.libsonnet'; 
local common = import './common.libsonnet';
local inputs = input_util.create_merge_input(common["num_keys"], 100, 20);
{
   "algos": {
      "common": common["algos"]["common"],
      "list": merge_algos.get_merge_algos(common["key_type"], common["key_size"], common["value_size"]),
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
   ],
   "report_format": "csv",
   "tests": inputs["tests"],
   "repeat": 5,
}

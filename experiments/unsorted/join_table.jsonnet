local test_output_dir = std.extVar("TEST_OUTPUT_DIR");
local repeats = std.parseInt(std.extVar("TEST_REPEAT"));
[
    {
        "algo": algo,
        "algo_name": algo, 
        "name": algo + "_run"+r,
        "check_checksum": false,
        "inner_table": std.extVar("TEST_INNER_TABLE"),
        "outer_table": std.extVar("TEST_OUTER_TABLE"),
        "result_path": test_output_dir + "/" + algo + "_run"+r,
        "load_sstable_in_mem": false,
        "num_threads": 1,
        "write_result_to_disk": true,
        "key_size": 8,
        "value_size": 8,
    } 
    for algo in ["unsorted_lsj", "unsorted_inlj"]
    for r in std.range(0, repeats)
]
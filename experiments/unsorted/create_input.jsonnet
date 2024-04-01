{
    "algo": "create_input",
    "method": "unsorted",
    "fraction_of_keys": std.parseJson(std.extVar("FRACTION")),
    "source": std.extVar("TEST_INPUT_FILE"),
    "result_path": std.extVar("TEST_OUTPUT_FILE"),
    "create_indexes": true,
}

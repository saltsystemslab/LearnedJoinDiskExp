{
    create_join_input(num_keys, num_common_keys, max_ratio, points)::
    local step = max_ratio / points;
    local ratios = std.map(function(x) std.ceil(step * x), std.range(1, points));
    local common_inputs = [
        {
            "name": "common",
            "num_keys": num_common_keys,
        },
        {
            "name": "all",
            "num_keys": num_keys,
            "common_keys_file": "common",
        }
    ];
    local inputs = [{"common_keys_file": "common", "name": std.toString(i), "num_keys": std.ceil(num_keys/i)}
        for i in ratios];
    {
        "input": common_inputs + inputs,
        "tests": {
            "list":
            [{
            "dir": std.toString(input.name),
            "inner_table": "all",
            "label_x": std.parseInt(input.name),
            "outer_table": input.name
        } for input in inputs]}
    }
}
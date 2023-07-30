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
    },
    create_low_join_input_sosd(num_keys, num_common_keys, max_ratio, points)::
    local step = max_ratio / points;
    local ratios = std.map(function(x) std.ceil(step * x), std.range(1, points));
    local common_inputs = [
        {
            "key_size": 8,
            "method": "select_common_keys",
            "name": "common",
            "num_keys": num_common_keys,
            "value_size": 0
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
    },
    create_all_join_input_sosd(num_keys, num_common_keys, max_ratio, points)::
    local step = max_ratio / points;
    local ratios = std.map(function(x) std.ceil(step * x), std.range(1, points));
    local common_inputs = [
        {
            "name": "all",
            "num_keys": 0,
            "use_all": true,
        }
    ];
    local inputs = [{"name": std.toString(i), "num_keys": std.ceil(num_keys/i)}
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
    },
    create_merge_input(num_keys, max_ratio, points)::
    local step = max_ratio / points;
    local ratios = std.map(function(x) std.ceil(step * x), std.range(1, points));
    local common_inputs = [{
            "name": "inner",
            "num_keys": num_keys,
        }
    ];
    local inputs = [{"name": std.toString(i), "num_keys": std.ceil(num_keys/i)}
        for i in ratios];
    {
        "input": common_inputs + inputs,
        "tests": {
            "list":
            [{
            "dir": std.toString(input.name),
            "inner_table": "inner",
            "label_x": std.parseInt(input.name),
            "outer_table": input.name
        } for input in inputs]}
    },

}
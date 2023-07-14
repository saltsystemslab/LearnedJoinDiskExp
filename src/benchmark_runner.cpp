#include <iostream>
#include <fstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Please specify input benchmark JSON spec file");
        abort();
    }
    std::string benchmark_spec_path(argv[1]);
    std::ifstream benchmark_ifstream(benchmark_spec_path);
    json benchmark_spec = json::parse(benchmark_ifstream);

    std::cout<<benchmark_spec["name"]<<std::endl;
    return 0;
}
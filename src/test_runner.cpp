#include "runner.h"
#include <fstream>
#include <iostream>
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
  json benchmark_output = li_merge::run_test(benchmark_spec);

  json result;
  result["spec"] = benchmark_spec;
  result["result"] = benchmark_output;
  printf("%s\n", result.dump().c_str());
  return 0;
}
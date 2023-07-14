cmake_minimum_required(VERSION 3.25)
project(LearnedIndexMerge)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(third_party/json)
add_executable(benchmark_runner src/benchmark_runner.cpp include/benchmark_spec.h include/interfaces/index.h include/interfaces/iterator.h)

target_link_libraries(benchmark_runner PRIVATE nlohmann_json::nlohmann_json)
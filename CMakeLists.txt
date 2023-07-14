cmake_minimum_required(VERSION 3.22)
project(LearnedIndexMerge)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(third_party/json)
# From: https://google.github.io/googletest/quickstart-cmake.html
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include_directories(include/interfaces)
include_directories(include/algos)

add_executable(benchmark_runner src/benchmark_runner.cpp include/interfaces/sstable.h)
target_link_libraries(benchmark_runner PRIVATE nlohmann_json::nlohmann_json)

enable_testing()
add_executable(join_test test/algos/join_test.cpp)
target_link_libraries(join_test GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(join_test)
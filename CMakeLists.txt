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
find_package(OpenSSL REQUIRED)

include_directories(
  include/
  include/algos
  include/benchmark
  include/interfaces
  include/indexes
  include/sstable
  include/betree/
  third_party/pgm/include
  third_party/LearnedIndexDiskExp/code/b+_tree
  third_party/LearnedIndexDiskExp/code/b+_tree/stx-btree-0.9/include
)

set(THIRD_PARTY GTest::gtest_main OpenSSL::SSL tbb nlohmann_json::nlohmann_json)

option(TRACK_STATS "TRACK STATS" OFF)
if (TRACK_STATS)
  add_compile_definitions(TRACK_STATS=1)
endif()


add_executable(benchmark_runner src/test_runner.cpp)
target_link_libraries(benchmark_runner PRIVATE ${THIRD_PARTY})

enable_testing()
add_executable(merge_test test/merge_test.cpp)
add_executable(in_mem_sstable_test test/in_mem_sstable_test.cpp)
add_executable(disk_sstable_test test/disk_sstable_test.cpp)
add_executable(synthetic_gen_test test/synthetic_gen_test.cpp)
add_executable(index_test test/index_test.cpp)
add_executable(test_runner test/test_runner.cpp)

set(LIBRARY_LIST GTest::gtest_main OpenSSL::SSL tbb nlohmann_json::nlohmann_json)
target_link_libraries(merge_test ${LIBRARY_LIST})
target_link_libraries(in_mem_sstable_test ${LIBRARY_LIST})
target_link_libraries(disk_sstable_test ${LIBRARY_LIST})
target_link_libraries(synthetic_gen_test ${LIBRARY_LIST})
target_link_libraries(index_test ${LIBRARY_LIST})
target_link_libraries(test_runner ${LIBRARY_LIST})

include(GoogleTest)
gtest_discover_tests(merge_test)
gtest_discover_tests(in_mem_sstable_test)
gtest_discover_tests(synthetic_gen_test)
gtest_discover_tests(index_test)
gtest_discover_tests(disk_sstable_test)
gtest_discover_tests(test_runner)

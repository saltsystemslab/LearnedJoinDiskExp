cmake_minimum_required(VERSION 3.22)
project(LearnedIndexMerge)

set(CMAKE_CXX_STANDARD 17)
set(CXX_STANDARD_REQUIRED True)

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
find_package(OpenMP REQUIRED)

include_directories(
  include/
  include/algos
  include/benchmark
  include/interfaces
  include/indexes
  include/sstable
  include/datastructures/betree/
  include/datastructures/btree/
  rmi/
  third_party/pgm/include
  third_party/LearnedIndexDiskExp/code/b+_tree/stx-btree-0.9/include
  third_party/LearnedIndexDiskExp/code/b+_tree/
  third_party/RadixSpline/include
  third_party/LearnedIndexDiskExp/code/ALEX/src/core
)

set(THIRD_PARTY GTest::gtest_main OpenSSL::SSL tbb nlohmann_json::nlohmann_json)

option(TRACK_STATS "TRACK STATS" OFF)
if (TRACK_STATS)
  add_compile_definitions(TRACK_STATS=1)
endif()

option(STRING_KEYS "STRING KEYS" OFF)
if (STRING_KEYS)
  add_compile_definitions(STRING_KEYS=1)
endif()

option(USE_ALEX "USE_ALEX" OFF)
if (USE_ALEX)
  add_compile_definitions(USE_ALEX=1)
endif()

add_executable(benchmark_runner src/test_runner.cpp )
#target_link_libraries(benchmark_runner PUBLIC OpenMP::OpenMP_CXX)
target_link_libraries(benchmark_runner PRIVATE ${THIRD_PARTY})


#include <fcntl.h>
#include <openssl/rand.h>
#include <stdio.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "comparator.h"
#include "config.h"
#include "iterator.h"
#include "iterator_with_model.h"
#include "learned_merge.h"
#include "learned_merge_bulk.h"
#include "model.h"
#include "parallel_learned_bulk_merge.h"
#include "parallel_learned_merge.h"
#include "parallel_standard_merge.h"
#include "sort_merge_binary_lookup.h"
#include "sort_merge_join.h"
#include "sort_merge_learned_join.h"
#include "standard_merge.h"
#include "test_input.h"
#include "uniform_random.h"

using namespace std;

static const int BUFFER_SIZE = 4096; // TODO: Make page buffer a flag.
static int FLAGS_num_of_lists = 2;
static bool FLAGS_disk_backed = false;
static bool FLAGS_print_result = false;
static bool FLAGS_print_input = false;
static vector<uint64_t> FLAGS_num_keys;
static MergeMode FLAGS_merge_mode = STANDARD_MERGE;
static int FLAGS_num_threads = 3;
static int FLAGS_num_sort_threads = 4;
static bool FLAGS_assert_sort = false;
static int FLAGS_PLR_error_bound = 10;
static uint64_t FLAGS_num_common_keys = 0;
#if USE_INT_128
static int FLAGS_key_size_bytes = 16;
#else
static int FLAGS_key_size_bytes = 8;
#endif
static std::string FLAGS_test_dir = "DB";
static std::string FLAGS_output_file = "result.txt";
// Can be "uniform", "ar", "osm"
static std::string FLAGS_dataset = "uniform";
// If dataset uniform, can be 'str', 'uint64_t', 'uint128_t'.
// Use key_size_bytes to set 'str' size.
static std::string FLAGS_key_type = "str";

bool is_flag(const char *arg, const char *flag) {
  return strncmp(arg, flag, strlen(flag)) == 0;
}

void print_flag_values() {
  printf("List Sizes: ");
  for (int i = 0; i < FLAGS_num_keys.size(); i++) {
    printf("%lu ", FLAGS_num_keys[i]);
  }
  printf("\n");
  printf("Use Disk: %d\n", FLAGS_disk_backed);
  printf("Key Bytes: %d\n", FLAGS_key_size_bytes);
  if (FLAGS_merge_mode == PARALLEL_LEARNED_MERGE ||
      FLAGS_merge_mode == PARALLEL_STANDARD_MERGE) {
    printf("Num Threads: %d\n", FLAGS_num_threads);
  }
  if (FLAGS_merge_mode == MERGE_WITH_MODEL ||
      FLAGS_merge_mode == PARALLEL_LEARNED_MERGE ||
      FLAGS_merge_mode == LEARNED_MERGE_JOIN) {
    printf("PLR_Error: %d\n", FLAGS_PLR_error_bound);
  }
}

void parse_flags(int argc, char **argv) {
  // TODO: Set default value for FLAGS_num_keys
  FLAGS_num_keys.push_back(10);
  FLAGS_num_keys.push_back(10);

  for (int i = 1; i < argc; i++) {
    double m;
    long long n;
    char junk;
    char *str = new char[100];
    if (sscanf(argv[i], "--num_of_lists=%lld%c", &n, &junk) == 1) {
      FLAGS_num_of_lists = n;
    } else if (sscanf(argv[i], "--key_bytes=%lld%c", &n, &junk) == 1) {
      // TODO: If Key Type is uint64_t or 128bit, then abort if attempting to
      // set key_size.
      FLAGS_key_size_bytes = n;
    } else if (is_flag(argv[i], "--num_keys=")) {
      FLAGS_num_keys.clear();
      char *FLAGS_num_keys_str = argv[i] + strlen("--num_keys=");
      stringstream ss(FLAGS_num_keys_str);
      while (ss.good()) {
        string substr;
        getline(ss, substr, ',');
        FLAGS_num_keys.push_back(stoull(substr));
      }
    } else if (sscanf(argv[i], "--use_disk=%lld%c", &n, &junk) == 1) {
      FLAGS_disk_backed = n;
    } else if (sscanf(argv[i], "--num_threads=%lld%c", &n, &junk) == 1) {
      FLAGS_num_threads = n;
    } else if (sscanf(argv[i], "--num_sort_threads=%lld%c", &n, &junk) == 1) {
      FLAGS_num_sort_threads = n;
    } else if (sscanf(argv[i], "--num_common_keys=%lld%c", &n, &junk) == 1) {
      FLAGS_num_common_keys = n;
    } else if (sscanf(argv[i], "--plr_error_bound=%lld%c", &n, &junk) == 1) {
      FLAGS_PLR_error_bound = n;
    } else if (sscanf(argv[i], "--print_input=%lld%c", &n, &junk) == 1) {
      FLAGS_print_input = n;
    } else if (sscanf(argv[i], "--print_result=%lld%c", &n, &junk) == 1) {
      FLAGS_print_result = n;
    } else if (sscanf(argv[i], "--assert_sort=%lld%c", &n, &junk) == 1) {
      FLAGS_assert_sort = n;
    } else if (sscanf(argv[i], "--dataset=%s", str) == 1) {
      std::string dataset(str);
      FLAGS_dataset = dataset;
    } else if (sscanf(argv[i], "--key_type=%s", str) == 1) {
      std::string key_type(str);
      FLAGS_key_type = key_type;
    } else if (sscanf(argv[i], "--test_dir=%s", str) == 1) {
      std::string test_dir(str);
      if (test_dir[test_dir.size() - 1] == '/') {
        printf("test_dir should not end with /\n");
        abort();
      }
      FLAGS_test_dir = test_dir;
    } else if (sscanf(argv[i], "--output_file=%s", str) == 1) {
      std::string output_file(str);
      FLAGS_output_file = output_file;
    } else if (sscanf(argv[i], "--merge_mode=%s", str) == 1) {
      if (strcmp(str, "standard") == 0) {
        FLAGS_merge_mode = STANDARD_MERGE;
      } else if (strcmp(str, "parallel_standard") == 0) {
        FLAGS_merge_mode = PARALLEL_STANDARD_MERGE;
      } else if (strcmp(str, "learned") == 0) {
        FLAGS_merge_mode = MERGE_WITH_MODEL;
      } else if (strcmp(str, "parallel_learned") == 0) {
        FLAGS_merge_mode = PARALLEL_LEARNED_MERGE;
      } else if (strcmp(str, "parallel_learned_bulk") == 0) {
        FLAGS_merge_mode = PARALLEL_LEARNED_MERGE_BULK;
      } else if (strcmp(str, "learned_bulk") == 0) {
        FLAGS_merge_mode = MERGE_WITH_MODEL_BULK;
      } else if (strcmp(str, "standard_join") == 0) {
        FLAGS_merge_mode = STANDARD_MERGE_JOIN;
      } else if (strcmp(str, "standard_join_binary") == 0) {
        FLAGS_merge_mode = STANDARD_MERGE_BINARY_LOOKUP_JOIN;
      } else if (strcmp(str, "learned_join") == 0) {
        FLAGS_merge_mode = LEARNED_MERGE_JOIN;
      } else if (strcmp(str, "no_op") == 0) {
        FLAGS_merge_mode = NO_OP;
      } else {
        abort();
      }
      printf("merge_mode: %s\n", str);
    } else {
      printf("WARNING: unrecognized flag %s\n", argv[i]);
      abort();
    }
  }
  if (FLAGS_num_keys.size() != 2) {
    printf("Number of lists must be 2 for now.");
    abort();
  }
  if (FLAGS_num_keys.size() != FLAGS_num_of_lists) {
    printf("Number of lists does not match num_keys flag");
    abort();
  }
  if (FLAGS_dataset != "uniform") {
    abort();
  }
  print_flag_values();
}

void init_test_dir() {
  std::string mkdir = "mkdir -p " + FLAGS_test_dir;
  system(mkdir.c_str());
  std::string rm_result = "rm -f " + FLAGS_test_dir + "/result";
  system(rm_result.c_str());
}

int main(int argc, char **argv) {
  printf("-----------------\n");
  parse_flags(argc, argv);
  init_test_dir();

  Comparator<Slice> *comp;
  SliceToPlrPointConverter *converter;
  if (FLAGS_key_type == "str") {
    comp = new SliceComparator();
    converter = new FixedKeySizeToPlrPointConverter();
  } else if (FLAGS_key_type == "uint64") {
    comp = new SliceAsUint64Comparator();
    converter = new SliceAsUint64PlrPointConverter();
  } else {
    printf("Unrecognized key type\n");
    abort();
  }

  BenchmarkInput input; 
  input.test_dir = FLAGS_test_dir;
  input.result_file = FLAGS_output_file;
  input.num_of_lists = FLAGS_num_keys.size();
  input.list_sizes = FLAGS_num_keys;
  input.key_size_bytes = FLAGS_key_size_bytes;
  input.plr_error_bound = FLAGS_PLR_error_bound;
  input.is_disk_backed = FLAGS_disk_backed;
  input.merge_mode = FLAGS_merge_mode;
  input.iterators_with_model = new IteratorWithModel<Slice> *[input.num_of_lists];
  input.comparator = comp;
  input.slice_to_point_converter = converter;
  input.num_common_keys = FLAGS_num_common_keys;

  fill_uniform_input_lists(&input);

  if (FLAGS_print_input) {
    printf("Printing input temporarily removed!");
    abort();
  }

  Iterator<Slice> **iterators = input.iterators;
  IteratorWithModel<Slice> **iterators_with_model =
      input.iterators_with_model;
  IteratorBuilder<Slice> *resultBuilder = input.resultBuilder;
  Comparator<Slice> *c = input.comparator;
  int num_of_lists = input.num_of_lists;

  Iterator<Slice> *result;
  auto merge_start = std::chrono::high_resolution_clock::now();
  switch (FLAGS_merge_mode) {
  case STANDARD_MERGE:
    result = StandardMerger<Slice>::merge(iterators, num_of_lists, c,
                                             resultBuilder);
    break;
  case PARALLEL_STANDARD_MERGE:
    if (num_of_lists != 2) {
      printf("Currently only 2 lists can be merged in parallel");
      abort();
    }
    result = ParallelStandardMerger<Slice>::merge(
        iterators[0], iterators[1], FLAGS_num_threads, c, resultBuilder);
    break;
  case PARALLEL_LEARNED_MERGE:
    if (num_of_lists != 2) {
      printf("Currently only 2 lists can be merged in parallel");
      abort();
    }
    result = ParallelLearnedMerger<Slice>::merge(
        iterators_with_model[0], iterators_with_model[1], FLAGS_num_threads, c,
        resultBuilder);
    break;
  case PARALLEL_LEARNED_MERGE_BULK:
    if (num_of_lists != 2) {
      printf("Currently only 2 lists can be merged in parallel");
      abort();
    }
    result = ParallelLearnedMergerBulk<Slice>::merge(
        iterators_with_model[0], iterators_with_model[1], 3, c, resultBuilder);
    break;
  case MERGE_WITH_MODEL:
    result = LearnedMerger<Slice>::merge(iterators_with_model, num_of_lists,
                                            c, resultBuilder);
    break;
  case MERGE_WITH_MODEL_BULK:
    result = LearnedMergerBulk<Slice>::merge(iterators_with_model,
                                                num_of_lists, c, resultBuilder);
    break;
  case STANDARD_MERGE_JOIN:
    result = SortedMergeJoin<Slice>::merge(iterators[0], iterators[1], c,
                                              resultBuilder);
    break;
  case STANDARD_MERGE_BINARY_LOOKUP_JOIN:
    result = SortedMergeBinaryLookupJoin<Slice>::merge(
        iterators[0], iterators[1], c, resultBuilder);
    break;
  case LEARNED_MERGE_JOIN:
    result = SortedMergeLearnedJoin<Slice>::merge(
        iterators_with_model[0], iterators_with_model[1], c, resultBuilder);
    break;
  case NO_OP:
    break;
  default:
    abort();
  }
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();

  float duration_sec = duration_ns / 1e9;
  printf("Merge duration: %.3lf s\n", duration_sec);
  std::cout << "Ok!" << std::endl;
  printf("-----------------\n");
}

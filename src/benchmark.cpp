
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
#include "int_array_iterator.h"
#include "int_comparator.h"
#include "int_disk_iterator.h"
#include "int_plr_model.h"
#include "iterator.h"
#include "iterator_with_model.h"
#include "learned_merge.h"
#include "learned_merge_bulk.h"
#include "model.h"
#include "parallel_learned_bulk_merge.h"
#include "parallel_learned_merge.h"
#include "parallel_standard_merge.h"
#include "slice_array_iterator.h"
#include "slice_file_iterator.h"
#include "slice_plr_model.h"
#include "sort_merge_binary_lookup.h"
#include "sort_merge_join.h"
#include "sort_merge_learned_join.h"
#include "standard_merge.h"
#include "uniform_random.h"
#include "test_input.h"
#include "sstable.h"

using namespace std;

static const int BUFFER_SIZE = 4096;  // TODO: Make page buffer a flag.
static int FLAGS_num_of_lists = 2;
static bool FLAGS_disk_backed = false;
static bool FLAGS_print_result = false;
static bool FLAGS_print_input = false;
static vector<uint64_t> FLAGS_num_keys;
static MERGE_MODE FLAGS_merge_mode = STANDARD_MERGE;
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

bool is_flag(const char *arg, const char *flag) {
  return strncmp(arg, flag, strlen(flag)) == 0;
}

#if !USE_STRING_KEYS
KEY_TYPE to_int(const char *s) {
  KEY_TYPE k = 0;
  // TODO: Check for endianness. Here we assume little endian.
  // TODO: Reverse using assembly instructions.
  for (int i = 0; i < FLAGS_key_size_bytes; i++) {
    uint8_t b = (uint8_t)s[i];
    k = (k << 8) + b;
  }
  return k;
}
#endif

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
  print_flag_values();
}


IteratorWithModel<KEY_TYPE> *build_iterator_with_model(
    int iter_idx, uint64_t num_keys, int key_bytes, char *common_keys,
    uint64_t num_common_keys, TestInput *test_input) {
  char *keys = read_or_create_sstable_into_mem(
      FLAGS_test_dir, "iter_" + std::to_string(iter_idx), num_keys, FLAGS_key_size_bytes, FLAGS_num_sort_threads);
  keys = merge(keys, num_keys, common_keys, num_common_keys, key_bytes);
  num_keys += num_common_keys;
  test_input->total_input_keys_cnt += num_keys;

  // Construct Model Builder.
  ModelBuilder<KEY_TYPE> *m;
  if (test_input->is_learned()) {
#if USE_STRING_KEYS
    m = new SlicePLRModelBuilder(FLAGS_PLR_error_bound);
#else
    m = new IntPLRModelBuilder<KEY_TYPE>(FLAGS_PLR_error_bound);
#endif
  } else {
    m = new DummyModelBuilder<KEY_TYPE>();
  }

  // Construct Iterator Builder.
  IteratorBuilder<KEY_TYPE> *iterator_builder;
  if (FLAGS_disk_backed) {
    // TODO: Move DB name generation to own function.
    std::string fileName = get_sstable_path(
        FLAGS_test_dir, "DB_" + std::to_string(iter_idx), num_keys, key_bytes);
#if USE_STRING_KEYS
    iterator_builder = new SliceFileIteratorBuilder(
        fileName.c_str(), BUFFER_SIZE, FLAGS_key_size_bytes, fileName);
#else
    iterator_builder =
        new IntDiskBuilder<KEY_TYPE>(fileName.c_str(), BUFFER_SIZE, fileName);
#endif
  } else {
    std::string iterName = "iter_" + std::to_string(iter_idx);
#if USE_STRING_KEYS
    iterator_builder =
        new SliceArrayBuilder(keys, num_keys, FLAGS_key_size_bytes, iterName);
#else
    iterator_builder =
        new IntDiskBuilder<KEY_TYPE>(iterName.c_str(), BUFFER_SIZE, iterName);
#endif
  }

  IteratorWithModelBuilder<KEY_TYPE> *builder =
      new IteratorWithModelBuilder<KEY_TYPE>(iterator_builder, m);

  auto iterator_build_start = std::chrono::high_resolution_clock::now();
  for (uint64_t j = 0; j < num_keys; j++) {
#if USE_STRING_KEYS
    builder->add(Slice(keys + j * FLAGS_key_size_bytes, FLAGS_key_size_bytes));
#else
    builder->add(to_int(keys + j * FLAGS_key_size_bytes));
#endif
  }

  IteratorWithModel<KEY_TYPE> *iterator_with_model = builder->finish();
  printf("Finished training model!\n");

  auto iterator_build_end = std::chrono::high_resolution_clock::now();
  uint64_t iterator_build_duration_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(iterator_build_end -
                                                           iterator_build_start)
          .count();
  float iterator_build_duration_sec = iterator_build_duration_ns / 1e9;
  printf("Iterator %d creation duration time: %.3lf sec\n", iter_idx,
         iterator_build_duration_sec);
  printf("Iterator %d model size bytes: %lu\n", iter_idx,
         iterator_with_model->model_size_bytes());

  return iterator_with_model;
}

TestInput prepare_input() {
  if (!std::filesystem::exists(FLAGS_test_dir)) {
    std::string command = "mkdir -p " + FLAGS_test_dir;
    system(command.c_str());
  }

  TestInput test_input;
  test_input.num_of_lists = FLAGS_num_keys.size();
  test_input.iterators_with_model =
      new IteratorWithModel<KEY_TYPE> *[test_input.num_of_lists];
  test_input.iterators = new Iterator<KEY_TYPE> *[test_input.num_of_lists];
  test_input.total_input_keys_cnt = 0;
  test_input.merge_mode = FLAGS_merge_mode;

#if USE_STRING_KEYS
  test_input.comparator = new SliceComparator();
#else
  test_input.comparator = new IntComparator<KEY_TYPE>();
#endif

  // Generate common keys to be inserted in both iterators.
  char *common_keys = read_or_create_sstable_into_mem(
      FLAGS_test_dir, "common_keys", FLAGS_num_common_keys, FLAGS_key_size_bytes, FLAGS_num_sort_threads);

  for (int i = 0; i < test_input.num_of_lists; i++) {
    test_input.iterators_with_model[i] = build_iterator_with_model(
        i, FLAGS_num_keys[i], FLAGS_key_size_bytes, common_keys,
        FLAGS_num_common_keys, &test_input);
    test_input.iterators[i] =
        test_input.iterators_with_model[i]->get_iterator();
  }

  uint64_t total_num_of_keys = test_input.total_input_keys_cnt;
  IteratorBuilder<KEY_TYPE> *resultBuilder;
  if (FLAGS_disk_backed) {
#if USE_STRING_KEYS
    std::string fileName = FLAGS_test_dir + "/" + FLAGS_output_file;
    resultBuilder = new SliceFileIteratorBuilder(
        fileName.c_str(), BUFFER_SIZE, FLAGS_key_size_bytes, "result");
#else
    std::string fileName = FLAGS_test_dir + "/" + FLAGS_output_file;
    resultBuilder =
        new IntDiskBuilder<KEY_TYPE>(fileName.c_str(), BUFFER_SIZE, "result");
#endif
  } else {
#if USE_STRING_KEYS
    resultBuilder = new SliceArrayBuilder(total_num_of_keys,
                                          FLAGS_key_size_bytes, "result");
#else
    resultBuilder = new IntArrayBuilder<KEY_TYPE>(total_num_of_keys, "0");
#endif
  }
  test_input.resultBuilder = resultBuilder;
  return test_input;
}

int main(int argc, char **argv) {
  printf("-----------------\n");
  parse_flags(argc, argv);
  TestInput input = prepare_input();

  if (FLAGS_print_input) {
    input.print_input_data();
  }

  Iterator<KEY_TYPE> **iterators = input.iterators;
  IteratorWithModel<KEY_TYPE> **iterators_with_model =
      input.iterators_with_model;
  IteratorBuilder<KEY_TYPE> *resultBuilder = input.resultBuilder;
  Comparator<KEY_TYPE> *c = input.comparator;
  int num_of_lists = input.num_of_lists;

  Iterator<KEY_TYPE> *result;
  auto merge_start = std::chrono::high_resolution_clock::now();
  switch (FLAGS_merge_mode) {
    case STANDARD_MERGE:
      result = StandardMerger<KEY_TYPE>::merge(iterators, num_of_lists, c,
                                               resultBuilder);
      break;
    case PARALLEL_STANDARD_MERGE:
      if (num_of_lists != 2) {
        printf("Currently only 2 lists can be merged in parallel");
        abort();
      }
      result = ParallelStandardMerger<KEY_TYPE>::merge(
          iterators[0], iterators[1], FLAGS_num_threads, c, resultBuilder);
      break;
    case PARALLEL_LEARNED_MERGE:
      if (num_of_lists != 2) {
        printf("Currently only 2 lists can be merged in parallel");
        abort();
      }
      result = ParallelLearnedMerger<KEY_TYPE>::merge(
          iterators_with_model[0], iterators_with_model[1], FLAGS_num_threads,
          c, resultBuilder);
      break;
    case PARALLEL_LEARNED_MERGE_BULK:
      if (num_of_lists != 2) {
        printf("Currently only 2 lists can be merged in parallel");
        abort();
      }
      result = ParallelLearnedMergerBulk<KEY_TYPE>::merge(
          iterators_with_model[0], iterators_with_model[1], 3, c,
          resultBuilder);
      break;
    case MERGE_WITH_MODEL:
      result = LearnedMerger<KEY_TYPE>::merge(iterators_with_model,
                                              num_of_lists, c, resultBuilder);
      break;
    case MERGE_WITH_MODEL_BULK:
      result = LearnedMergerBulk<KEY_TYPE>::merge(
          iterators_with_model, num_of_lists, c, resultBuilder);
      break;
    case STANDARD_MERGE_JOIN:
      result = SortedMergeJoin<KEY_TYPE>::merge(iterators[0], iterators[1], c,
                                                resultBuilder);
      break;
    case STANDARD_MERGE_BINARY_LOOKUP_JOIN:
      result = SortedMergeBinaryLookupJoin<KEY_TYPE>::merge(
          iterators[0], iterators[1], c, resultBuilder);
      break;
    case LEARNED_MERGE_JOIN:
      result = SortedMergeLearnedJoin<KEY_TYPE>::merge(
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
  if (FLAGS_print_result) {
    result->seekToFirst();
    printf("Merged List: %lu\n", result->num_keys());
    while (result->valid()) {
#if USE_STRING_KEYS
      std::string key_str = result->key().toString();
      printf("%s\n", key_str.c_str());
#elif USE_INT_128
      std::string key_str = int128_to_string(result->key());
      printf("%s\n", key_str.c_str());
#else
      std::string key_str = uint64_to_string(result->key());
      printf("%s\n", key_str.c_str());
#endif
      result->next();
    }
  }

  float duration_sec = duration_ns / 1e9;
  printf("Merge duration: %.3lf s\n", duration_sec);
  std::cout << "Ok!" << std::endl;
  printf("-----------------\n");
}

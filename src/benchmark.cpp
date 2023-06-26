
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

using namespace std;

enum MERGE_MODE {
  NO_OP,
  STANDARD_MERGE,
  PARALLEL_STANDARD_MERGE,
  PARALLEL_LEARNED_MERGE,
  PARALLEL_LEARNED_MERGE_BULK,
  MERGE_WITH_MODEL,
  MERGE_WITH_MODEL_BULK,
  STANDARD_MERGE_JOIN,
  STANDARD_MERGE_BINARY_LOOKUP_JOIN,
  LEARNED_MERGE_JOIN
};

static const int BUFFER_SIZE = 4096;  // TODO: Make page buffer a flag.
static int FLAGS_num_of_lists = 2;
static bool FLAGS_disk_backed = false;
static bool FLAGS_print_result = false;
static bool FLAGS_print_input = false;
static vector<uint64_t> FLAGS_num_keys;
static int FLAGS_merge_mode = STANDARD_MERGE;
static int FLAGS_num_threads = 3;
static bool FLAGS_assert_sort = false;
static int FLAGS_PLR_error_bound = 10;
static uint64_t FLAGS_num_common_keys = 3;
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

#if !USE_STRING_KEYS && USE_INT_128
std::string int128_to_string(KEY_TYPE k) {
  std::string s;
  while (k) {
    int d = (int)(k % 10);
    s = std::to_string(d) + s;
    k = k / 10;
  }
  return s;
}
#endif

#if !USE_STRING_KEYS && !USE_INT_128
std::string uint64_to_string(KEY_TYPE k) {
  std::string s;
  while (k) {
    int d = (int)(k % 10);
    s = std::to_string(d) + s;
    k = k / 10;
  }
  return s;
}
#endif

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
    }
    if (sscanf(argv[i], "--key_bytes=%lld%c", &n, &junk) == 1) {
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
  if (FLAGS_num_keys.size() != FLAGS_num_of_lists) {
    printf("Number of lists does not match num_keys flag");
    abort();
  }
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

int main(int argc, char **argv) {
  printf("-----------------\n");
  parse_flags(argc, argv);

  if (!std::filesystem::exists(FLAGS_test_dir)) {
    std::string command = "mkdir -p " + FLAGS_test_dir;
    system(command.c_str());
  }

  // TODO: Move SSTable name generation to own function.
  std::string common_keys_sstable =
      FLAGS_test_dir + "/common_" + std::to_string(FLAGS_num_common_keys) +
      "_" + std::to_string(FLAGS_key_size_bytes) + ".txt";
  char *common_keys = generate_keys(common_keys_sstable, FLAGS_num_common_keys,
                                    FLAGS_key_size_bytes);

  vector<std::string> correct;
  int num_of_lists = FLAGS_num_keys.size();
  IteratorWithModel<KEY_TYPE> **iterators_with_model =
      new IteratorWithModel<KEY_TYPE> *[num_of_lists];
  uint64_t total_num_of_keys = 0;
  for (int i = 0; i < num_of_lists; i++) {
    ModelBuilder<KEY_TYPE> *m;
    if (FLAGS_merge_mode == STANDARD_MERGE ||
        FLAGS_merge_mode == PARALLEL_STANDARD_MERGE ||
        FLAGS_merge_mode == STANDARD_MERGE_JOIN ||
        FLAGS_merge_mode == STANDARD_MERGE_BINARY_LOOKUP_JOIN
    ) {
      m = new DummyModelBuilder<KEY_TYPE>();
    } else {
#if USE_STRING_KEYS
      m = new SlicePLRModelBuilder(FLAGS_PLR_error_bound);
#else
      m = new IntPLRModelBuilder<KEY_TYPE>(FLAGS_PLR_error_bound);
#endif
    }
    // TODO: Move SSTable name generation to own function.
    std::string sstable_name = FLAGS_test_dir + "/" + std::to_string(i) + "_" +
                               std::to_string(FLAGS_num_keys[i]) + "_" +
                               std::to_string(FLAGS_key_size_bytes) + ".txt";

    char *keys =
        generate_keys(sstable_name, FLAGS_num_keys[i], FLAGS_key_size_bytes);

    keys = merge(keys, FLAGS_num_keys[i], common_keys, FLAGS_num_common_keys,
                 FLAGS_key_size_bytes);
    FLAGS_num_keys[i] += FLAGS_num_common_keys;
    total_num_of_keys += FLAGS_num_keys[i];

    IteratorBuilder<KEY_TYPE> *iterator_builder;
    if (FLAGS_disk_backed) {
      // TODO: Move DB name generation to own function.
      std::string fileName =
          FLAGS_test_dir + "/" + std::to_string(i + 1) + ".txt";
      std::cout << fileName << std::endl;
#if USE_STRING_KEYS
      iterator_builder = new SliceFileIteratorBuilder(
          fileName.c_str(), BUFFER_SIZE, FLAGS_key_size_bytes, fileName);
#else
      iterator_builder =
          new IntDiskBuilder<KEY_TYPE>(fileName.c_str(), BUFFER_SIZE, fileName);
#endif
    } else {
      std::string iterName = "iter_" + std::to_string(i + 1);
      std::cout << iterName << std::endl;
#if USE_STRING_KEYS
      iterator_builder = new SliceArrayBuilder(keys, FLAGS_num_keys[i],
                                               FLAGS_key_size_bytes, iterName);
#else
      iterator_builder =
          new IntDiskBuilder<KEY_TYPE>(iterName.c_str(), BUFFER_SIZE, iterName);
#endif
    }
    IteratorWithModelBuilder<KEY_TYPE> *builder =
        new IteratorWithModelBuilder<KEY_TYPE>(iterator_builder, m);

    auto iterator_build_start = std::chrono::high_resolution_clock::now();
    for (uint64_t j = 0; j < FLAGS_num_keys[i]; j++) {
      // There is a bit of a hack going on here for array based iterators.
      // The array pointer containing keys was already passed to the builder,
      // So Add() here does not actually add to iterator, but training happens
      // here.
#if USE_STRING_KEYS
      builder->add(
          Slice(keys + j * FLAGS_key_size_bytes, FLAGS_key_size_bytes));
#else
      builder->add(to_int(keys + j * FLAGS_key_size_bytes));
#endif

      if (FLAGS_assert_sort) {
        std::string key(keys + j * FLAGS_key_size_bytes, FLAGS_key_size_bytes);
        correct.push_back(key);
      }
    }
    iterators_with_model[i] = builder->finish();

    auto iterator_build_end = std::chrono::high_resolution_clock::now();
    uint64_t iterator_build_duration_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            iterator_build_end - iterator_build_start)
            .count();
    float iterator_build_duration_sec = iterator_build_duration_ns / 1e9;
    printf("Iterator %d creation duration time: %.3lf sec\n", i,
           iterator_build_duration_sec);
    printf("Iterator %d model size bytes: %lu\n", i,
           iterators_with_model[i]->model_size_bytes());
  }
  Iterator<KEY_TYPE> **iterators = new Iterator<KEY_TYPE> *[num_of_lists];
  for (int i = 0; i < num_of_lists; i++) {
    iterators[i] = iterators_with_model[i]->get_iterator();
  }

  if (FLAGS_print_input) {
    for (int i = 0; i < num_of_lists; i++) {
      Iterator<KEY_TYPE> *iter = iterators[i];
      iter->seekToFirst();
      printf("List %d\n", i);
      while (iter->valid()) {
#if USE_STRING_KEYS
        std::string key_str = iter->key().toString();
#elif USE_INT_128
        std::string key_str = int128_to_string(iter->key());
#else
        std::string key_str = uint64_to_string(iter->key());
#endif
        printf("%d Key: %lu %s\n", i, iter->current_pos(), key_str.c_str());
        iter->next();
      }
    }
  }

  if (FLAGS_assert_sort) {
    sort(correct.begin(), correct.end());
  }

  printf("Finished training models!\n");
#if USE_STRING_KEYS
  Comparator<KEY_TYPE> *c = new SliceComparator();
#else
  Comparator<KEY_TYPE> *c = new IntComparator<KEY_TYPE>();
#endif
  IteratorBuilder<KEY_TYPE> *resultBuilder;
  if (FLAGS_disk_backed) {
#if USE_STRING_KEYS
    std::string fileName = FLAGS_test_dir + "/" + FLAGS_output_file;
    printf("%s\n", fileName.c_str());
    resultBuilder = new SliceFileIteratorBuilder(
        fileName.c_str(), BUFFER_SIZE, FLAGS_key_size_bytes, "result");
#else
    std::string fileName = FLAGS_test_dir + "/" + FLAGS_output_file;
    printf("%s\n", fileName.c_str());
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
      result = SortedMergeBinaryLookupJoin<KEY_TYPE>::merge(iterators[0], iterators[1], c,
                                                resultBuilder);
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

  if (FLAGS_assert_sort) {
    result->seekToFirst();
    auto correctIterator = correct.begin();
    while (result->valid()) {
#if USE_STRING_KEYS
      std::string k1 = std::string(result->key().data_, result->key().size_);
      std::string k2 = *correctIterator;
#else
      KEY_TYPE k1 = result->key();
      KEY_TYPE k2 = to_int((*correctIterator).c_str());
#endif
      if (k1 != k2) {
        std::cout << correctIterator - correct.begin() << std::endl;
        abort();
      }
      result->next();
      correctIterator++;
    }
    assert(correctIterator == correct.end());
  }

  float duration_sec = duration_ns / 1e9;
  printf("Merge duration: %.3lf s\n", duration_sec);
  std::cout << "Ok!" << std::endl;
  printf("-----------------\n");
}

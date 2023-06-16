#include <bits/stdc++.h>
#include <fcntl.h>
#include <openssl/rand.h>
#include <stdio.h>

#include <cassert>
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
#include "slice_plr_model.h"
#include "slice_file_iterator.h"
#include "slice_array_iterator.h"
#include "standard_merge.h"
#include "uniform_random.h"

using namespace std;

enum MERGE_MODE {
  STANDARD_MERGE,
  PARALLEL_STANDARD_MERGE,
  PARALLEL_LEARNED_MERGE,
  PARALLEL_LEARNED_MERGE_BULK,
  MERGE_WITH_MODEL,
  MERGE_WITH_MODEL_BULK,
};

static const int BUFFER_SIZE = 4096;
static int FLAGS_num_of_lists = 2;
static bool FLAGS_disk_backed = false;
static bool FLAGS_print_result = false;
static vector<int> FLAGS_num_keys;
static int FLAGS_merge_mode = STANDARD_MERGE;
static int FLAGS_num_threads = 3;

bool is_flag(const char *arg, const char *flag) {
  return strncmp(arg, flag, strlen(flag)) == 0;
}

void parse_flags(int argc, char **argv) {
  // Set default vaule for FLAGS_num_keys
  FLAGS_num_keys.push_back(10);
  FLAGS_num_keys.push_back(10);

  for (int i = 1; i < argc; i++) {
    double m;
    long long n;
    char junk;
    char *str = new char[100];
    if (sscanf(argv[i], "--num_of_lists=%lld%c", &n, &junk) == 1) {
      FLAGS_num_of_lists = n;
    } else if (is_flag(argv[i], "--num_keys=")) {
      FLAGS_num_keys.clear();
      char *FLAGS_num_keys_str = argv[i] + strlen("--num_keys=");
      stringstream ss(FLAGS_num_keys_str);
      while (ss.good()) {
        string substr;
        getline(ss, substr, ',');
        FLAGS_num_keys.push_back(stoi(substr));
      }
    } else if (sscanf(argv[i], "--use_disk=%lld%c", &n, &junk) == 1) {
      FLAGS_disk_backed = n;
    } else if (sscanf(argv[i], "--num_threads=%lld%c", &n, &junk) == 1) {
      FLAGS_num_threads = n;
    } else if (sscanf(argv[i], "--print_result=%lld%c", &n, &junk) == 1) {
      FLAGS_print_result = n;
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
      } else {
        abort();
      }
    } else {
      printf("WARNING: unrecognized flag %s\n", argv[i]);
      abort();
    }
  }
  if (FLAGS_num_keys.size() != FLAGS_num_of_lists) {
    printf("Number of lists does not match num_keys flag");
    abort();
  }
}

#if USE_INT_128
static int FLAGS_key_size_bytes = 16;
#else
static int FLAGS_key_size_bytes = 8;
#endif

int main(int argc, char **argv) {
  parse_flags(argc, argv);
  if (FLAGS_disk_backed) {
    system("rm -rf DB && mkdir -p DB");
  }

#if ASSERT_SORT
  vector<KEY_TYPE> correct;
#endif
  int num_of_lists = FLAGS_num_keys.size();
  IteratorWithModel<KEY_TYPE> **iterators_with_model =
      new IteratorWithModel<KEY_TYPE> *[num_of_lists];
  Iterator<KEY_TYPE> **iterators = new Iterator<KEY_TYPE> *[num_of_lists];
  int total_num_of_keys = 0;
  for (int i = 0; i < num_of_lists; i++) {
    ModelBuilder<KEY_TYPE> *m = new SlicePLRModelBuilder();
    IteratorBuilder<KEY_TYPE> *iterator_builder;
    if (FLAGS_disk_backed) {
      std::string fileName = "./DB/" + std::to_string(i + 1) + ".txt";
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
      iterator_builder = new SliceArrayBuilder(
          FLAGS_num_keys[i], FLAGS_key_size_bytes, iterName);
#else
      abort();
#endif
    }
    IteratorWithModelBuilder<KEY_TYPE> *builder =
        new IteratorWithModelBuilder<KEY_TYPE>(iterator_builder, m);
    vector<std::string> keys = generate_keys(FLAGS_num_keys[i], FLAGS_key_size_bytes);
    for (int j = 0; j < FLAGS_num_keys[i]; j++) {
#if USE_STRING_KEYS
      builder->add(Slice(keys[j].c_str(), FLAGS_key_size_bytes));
#else
      builder->add(to_int(keys[j]);
#endif

#if ASSERT_SORT
      correct.push_back(keys[j]);
#endif
    }
    iterators_with_model[i] = builder->finish();
    iterators[i] = iterators_with_model[i]->get_iterator();
    total_num_of_keys += FLAGS_num_keys[i];
  }

  if (FLAGS_print_result) {
    for (int i = 0; i < num_of_lists; i++) {
      Iterator<KEY_TYPE> *iter = iterators[i];
      iter->seekToFirst();
      printf("List %d\n", i);
      while (iter->valid()) {
#if USE_STRING_KEYS
        std::string key_str = iter->key().toString();
#else
        std::string key_str = std::to_string(iter->key());
#endif
        printf("%d Key: %lu %s\n", i, iter->current_pos(), key_str.c_str());
        iter->next();
      }
    }
  }
#if ASSERT_SORT
  sort(correct.begin(), correct.end());
#endif

  printf("Finished training models!\n");
#if USE_STRING_KEYS
  Comparator<KEY_TYPE> *c = new SliceComparator();
#else
  Comparator<KEY_TYPE> *c = new IntComparator<KEY_TYPE>();
#endif
  IteratorBuilder<KEY_TYPE> *resultBuilder;
  if (FLAGS_disk_backed) {
#if USE_STRING_KEYS
    resultBuilder = new SliceFileIteratorBuilder(
        "./DB/result.txt", BUFFER_SIZE, FLAGS_key_size_bytes, "result");
#else
    resultBuilder =
        new Slice<KEY_TYPE>("./DB/result.txt", BUFFER_SIZE, "result");
#endif
  } else {
#if USE_STRING_KEYS
      resultBuilder = new SliceArrayBuilder(
          total_num_of_keys, FLAGS_key_size_bytes, "result");
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
      result = ParallelLearnedMerger<KEY_TYPE>::merge(iterators_with_model[0],
                                                      iterators_with_model[1],
                                                      FLAGS_num_threads, c, resultBuilder);
      break;
    case PARALLEL_LEARNED_MERGE_BULK:
      if (num_of_lists != 2) {
        printf("Currently only 2 lists can be merged in parallel");
        abort();
      }
      result = ParallelLearnedMergerBulk<KEY_TYPE>::merge(iterators_with_model[0],
                                                      iterators_with_model[1],
                                                      3, c, resultBuilder);
      break;
    case MERGE_WITH_MODEL:
      result = LearnedMerger<KEY_TYPE>::merge(iterators_with_model,
                                              num_of_lists, c, resultBuilder);
      break;
    case MERGE_WITH_MODEL_BULK:
      result = LearnedMergerBulk<KEY_TYPE>::merge(
          iterators_with_model, num_of_lists, c, resultBuilder);
  }
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  if (FLAGS_print_result) {
    printf("Merged List: %lu\n", result->num_keys());
    while (result->valid()) {
#if USE_STRING_KEYS
      std::string key_str = result->key().toString();
      printf("%s\n", key_str.c_str());
#else
      std::string key_str = std::to_string(result->key());
      printf("%s\n", key_str.c_str());
#endif
      result->next();
    }
  }

#if ASSERT_SORT
  result->seekToFirst();
  auto correctIterator = correct.begin();
  while (result->valid()) {
    KEY_TYPE k1 = result->key();
    KEY_TYPE k2 = *correctIterator;
    if (k1 != k2) {
      std::cout<<correctIterator - correct.begin()<<std::endl;
      abort();
    }
    result->next();
    correctIterator++;
  }
  assert(correctIterator == correct.end());
#endif

  float duration_sec = duration_ns / 1e9;
  printf("Merge duration: %.3lf s\n", duration_sec);
  std::cout << "Ok!" << std::endl;
}

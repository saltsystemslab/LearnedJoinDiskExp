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
#include "int_pgm_index.h"
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
#include "slice_pgm_index.h"
#include "standard_merge.h"
#include "uniform_random.h"
#include "learned_lookup.h"
#include "standard_lookup.h"
#include "int_binary_search.h"
//#include "slice_binary_search.h"

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
static bool FLAGS_disk_backed = false;
static bool FLAGS_print_result = false;
//static vector<int> FLAGS_num_keys;
static int FLAGS_num_keys = 10000;
static int FLAGS_num_queries = 10000;
static int FLAGS_merge_mode = STANDARD_MERGE;
static int FLAGS_num_threads = 3;
static bool FLAGS_assert_sort = false;
static int FLAGS_PLR_error_bound = 10;
#if USE_INT_128
static int FLAGS_key_size_bytes = 16;
#else
static int FLAGS_key_size_bytes = 8;
#endif

bool is_flag(const char *arg, const char *flag) {
  return strncmp(arg, flag, strlen(flag)) == 0;
}

#if !USE_STRING_KEYS
KEY_TYPE to_int(std::string s) {
  KEY_TYPE k = 0;
  // CHECK FOR ENDIANESS.
  for (int i = 0; i < FLAGS_key_size_bytes; i++) {
    uint8_t b = (uint8_t)s.c_str()[i];
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

void parse_flags(int argc, char **argv) {
  // Set default vaule for FLAGS_num_keys
  

  for (int i = 1; i < argc; i++) {
    double m;
    long long n;
    char junk;
    char *str = new char[100];
    if (sscanf(argv[i], "--key_bytes=%lld%c", &n, &junk) == 1) {
      FLAGS_key_size_bytes = n;
    } else if (sscanf(argv[i], "--num_keys=%lld%c", &n, &junk) == 1) {
      FLAGS_num_keys = n;
    }
      else if (sscanf(argv[i], "--num_queries=%lld%c", &n, &junk) == 1) {
      FLAGS_num_queries = n;
    } else if (sscanf(argv[i], "--use_disk=%lld%c", &n, &junk) == 1) {
      FLAGS_disk_backed = n;
    } else if (sscanf(argv[i], "--num_threads=%lld%c", &n, &junk) == 1) {
      FLAGS_num_threads = n;
    } else if (sscanf(argv[i], "--plr_error_bound=%lld%c", &n, &junk) == 1) {
      FLAGS_PLR_error_bound = n;
    } else if (sscanf(argv[i], "--print_result=%lld%c", &n, &junk) == 1) {
      FLAGS_print_result = n;
    } else if (sscanf(argv[i], "--assert_sort=%lld%c", &n, &junk) == 1) {
      FLAGS_assert_sort = n;
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

  
  printf("\n");
  printf("Use Disk: %d\n", FLAGS_disk_backed);
  printf("Key Bytes: %d\n", FLAGS_key_size_bytes);
  printf("PLR_Error: %d\n", FLAGS_PLR_error_bound);
  if (FLAGS_merge_mode == PARALLEL_LEARNED_MERGE || FLAGS_merge_mode == PARALLEL_STANDARD_MERGE) {
    printf("Num Threads: %d\n", FLAGS_num_threads);
  }
}

int main(int argc, char **argv) {
  printf("-----------------\n");
  parse_flags(argc, argv);
  if (FLAGS_disk_backed) {
    system("rm -rf DB && mkdir -p DB");
  }

  vector<std::string> correct;
  size_t segmentCount=0;
  IteratorWithModel<KEY_TYPE> *iterator_with_model;
    ModelBuilder<KEY_TYPE> *m;
    if (FLAGS_merge_mode == STANDARD_MERGE || FLAGS_merge_mode == PARALLEL_STANDARD_MERGE) {
      m = new DummyModelBuilder<KEY_TYPE>();
    } else {
#if USE_STRING_KEYS
      #if USE_PLR_MODEL
      m = new SlicePLRModelBuilder(FLAGS_PLR_error_bound);
      #endif
      #if USE_PGM_INDEX
      m = new SlicePGMIndexBuilder(FLAGS_num_keys);
      #endif
      #if USE_BINARY_SEARCH
      m = new SliceBinarySearchBuilder(FLAGS_num_keys);
    #endif
#else
      #if USE_PLR_MODEL
      m = new IntPLRModelBuilder<KEY_TYPE>(FLAGS_PLR_error_bound);
      #endif
      #if USE_PGM_INDEX
      m = new IntPGMIndexBuilder<KEY_TYPE>(FLAGS_num_keys);
      #endif
      #if USE_BINARY_SEARCH
      m = new IntBinarySearchBuilder<KEY_TYPE>(FLAGS_num_keys);
      #endif
#endif
    }
    IteratorBuilder<KEY_TYPE> *iterator_builder;
    if (FLAGS_disk_backed) {
      std::string fileName = "./DB/" + std::to_string(0 + 1) + ".txt";
      std::cout << fileName << std::endl;
#if USE_STRING_KEYS
      iterator_builder = new SliceFileIteratorBuilder(
          fileName.c_str(), BUFFER_SIZE, FLAGS_key_size_bytes, fileName);
#else
      iterator_builder =
          new IntDiskBuilder<KEY_TYPE>(fileName.c_str(), BUFFER_SIZE, fileName);
#endif
    } else {
      std::string iterName = "iter_" + std::to_string(0 + 1);
      std::cout << iterName << std::endl;
#if USE_STRING_KEYS
      iterator_builder = new SliceArrayBuilder(FLAGS_num_keys,
                                               FLAGS_key_size_bytes, iterName);
#else
      iterator_builder =
          new IntDiskBuilder<KEY_TYPE>(iterName.c_str(), BUFFER_SIZE, iterName);
#endif
    }
    IteratorWithModelBuilder<KEY_TYPE> *builder =
        new IteratorWithModelBuilder<KEY_TYPE>(iterator_builder, m);
    vector<std::string> keys =
        generate_keys(FLAGS_num_keys, FLAGS_key_size_bytes);

    vector<std::string> queries =
        generate_keys(FLAGS_num_queries, FLAGS_key_size_bytes);
      
    queries.insert(queries.end(), keys.begin(), keys.end());

    auto iterator_build_start = std::chrono::high_resolution_clock::now();
    for (int j = 0; j < FLAGS_num_keys; j++) {
#if USE_STRING_KEYS
      builder->add(Slice(keys[j].c_str(), FLAGS_key_size_bytes));
#else
      builder->add(to_int(keys[j]));
#endif

      if (FLAGS_assert_sort) {
        correct.push_back(keys[j]);
      }
    }
    iterator_with_model = builder->finish();
    segmentCount = iterator_with_model->model_size_bytes();
    printf("Number of segments: %ld\n", segmentCount);
    
    auto iterator_build_end = std::chrono::high_resolution_clock::now();
    uint64_t iterator_build_duration_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(iterator_build_end -
                                                             iterator_build_start).count();
    float iterator_build_duration_sec = iterator_build_duration_ns / 1e9;
    //printf("Iterator %d creation duration time: %.3lf sec\n", i, iterator_build_duration_sec);
   // printf("Iterator %d model size bytes: %lu\n", i, iterators_with_model->model_size_bytes());

  #if USE_STRING_KEYS
      Comparator<KEY_TYPE> *c = new SliceComparator();
    #else
      Comparator<KEY_TYPE> *c = new IntComparator<KEY_TYPE>();
    #endif
 
  auto lookup_start = std::chrono::high_resolution_clock::now();

for(int i=0; i<queries.size(); i++) {
  #if USE_STRING_KEYS
    KEY_TYPE query = Slice(queries[i].c_str(), FLAGS_key_size_bytes);
  #else
    KEY_TYPE query = to_int(queries[i]);
  #endif
  
  #if USE_PLR_MODEL || USE_PGM_INDEX 
    bool status = LearnedLookup::lookup<KEY_TYPE>(iterator_with_model, FLAGS_num_keys, c, query);
    if(status == true) {
        assert(std::find(keys.begin(), keys.end(), queries[i]) !=
        keys.end());
    }
    else {
        assert(std::find(keys.begin(), keys.end(), queries[i]) ==
        keys.end());
    }
  #endif
  #if USE_BINARY_SEARCH
    bool status =
        StandardLookup::lookup<KEY_TYPE>(iterator_with_model, c, query);
    if(status == true) {
        assert(std::find(keys.begin(), keys.end(), queries[i]) !=
        keys.end());
    }
    else {

        assert(std::find(keys.begin(), keys.end(), queries[i]) ==
        keys.end());
    }
  #endif
}
  auto lookup_end = std::chrono::high_resolution_clock::now();
  auto lookup_duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         lookup_end - lookup_start)
                         .count();



  float lookup_duration_sec = lookup_duration_ns / 1e9;
  printf("Lookup duration: %.3lf s\n", lookup_duration_sec);
  std::cout << "Ok!" << std::endl;
  printf("-----------------\n");
}

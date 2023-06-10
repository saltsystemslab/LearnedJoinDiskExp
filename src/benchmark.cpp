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
#include "int_disk_iterator.h"
#include "int_comparator.h"
#include "iterator.h"
#include "iterator_with_model.h"
#include "learned_merge.h"
#include "model.h"
#include "plr_model.h"
#include "standard_merge.h"

using namespace std;

enum MERGE_MODE {
  STANDARD_MERGE,
  MERGE_WITH_MODEL,
  MERGE_WITH_MODEL_BULK,
};

#if USE_INT_128 
static int FLAGS_key_size_bytes = 16;
#else
static int FLAGS_key_size_bytes = 8;
#endif

static const int BUFFER_SIZE = 1000;
static int FLAGS_num_of_lists = 2;
static KEY_TYPE FLAGS_universe_size = 2000000000000000;
static bool FLAGS_disk_backed = false;
static bool FLAGS_print_result = false;
static const char *FLAGS_num_keys = "10,10";
static const char *FLAGS_DB_dir = "./DB";
static int FLAGS_merge_mode = STANDARD_MERGE;

void rand_bytes(unsigned char *v, size_t n) {
  uint64_t i = 0;
  int fd = open("/dev/random", O_RDONLY);
  read(fd, v, n);
}

vector<KEY_TYPE> generate_keys(uint64_t num_keys, KEY_TYPE universe) {
  uint64_t bytes_to_alloc = num_keys * sizeof(universe);
  unsigned char *rand_nums = new unsigned char[bytes_to_alloc];
  rand_bytes(rand_nums, bytes_to_alloc);

  vector<KEY_TYPE> keys;
  for (uint64_t i = 0; i < bytes_to_alloc; i += sizeof(universe)) {
    KEY_TYPE k = *(KEY_TYPE *)(rand_nums + i);
    keys.push_back(k);
  }
  delete rand_nums;
  printf("Done generating random bytes\n");
  sort(keys.begin(), keys.end());
  printf("Done adding to vector and sorting!\n");
  return keys;
}

string to_str(KEY_TYPE k) {
  std::string r;
  while (k) {
    r.push_back('0' + k % 10);
    k = k / 10;
  }
  reverse(r.begin(), r.end());
  return r;
}

string to_fixed_size_key(KEY_TYPE key_value, int key_size) {
  string key = to_str(key_value);
  string result = string(key_size - key.length(), '0') + key;
  return std::move(result);
}

bool is_flag(const char *arg, const char *flag) {
  return strncmp(arg, flag, strlen(flag)) == 0;
}

int main(int argc, char **argv) {
  FLAGS_universe_size = 1;
  for (int i = 0; i < FLAGS_key_size_bytes * 8 - 1; i++) {
    FLAGS_universe_size = FLAGS_universe_size << 1;
  }
  for (int i = 1; i < argc; i++) {
    double m;
    long long n;
    char junk;
    char str[100];
    if (sscanf(argv[i], "--num_of_lists=%lld%c", &n, &junk) == 1) {
      FLAGS_num_of_lists = n;
    } else if (is_flag(argv[i], "--num_keys=")) {
      FLAGS_num_keys = argv[i] + strlen("--num_keys=");
    } else if (sscanf(argv[i], "--universe_log=%lld%c", &n, &junk) == 1) {
      FLAGS_universe_size = 1;
      for (int i = 0; i < n; i++) {
        FLAGS_universe_size = FLAGS_universe_size << 1;
      }
    } else if (sscanf(argv[i], "--use_disk=%lld%c", &n, &junk) == 1) {
      FLAGS_disk_backed = n;
    } else if (sscanf(argv[i], "--print_result=%lld%c", &n, &junk) == 1) {
      FLAGS_print_result = n;
    } else if (sscanf(argv[i], "--merge_mode=%s", &str) == 1) {
      if (strcmp(str, "standard") == 0) {
        FLAGS_merge_mode = STANDARD_MERGE;
      } 
      else if (strcmp(str, "learned") == 0) {
        FLAGS_merge_mode = MERGE_WITH_MODEL;
      } 
      else if (strcmp(str, "learned_bulk") == 0) {
        FLAGS_merge_mode = MERGE_WITH_MODEL_BULK;
      } 
      else {
        abort();
      }
    } else {
      printf("WARNING: unrecognized flag %s\n", argv[i]);
    }
  }

  if (FLAGS_disk_backed) {
    system("mkdir -p DB");
  }

  std::cout << "Universe Size: " << to_str(FLAGS_universe_size) << std::endl;

  vector<int> num_keys;
  stringstream ss(FLAGS_num_keys);

  while (ss.good()) {
    string substr;
    getline(ss, substr, ',');
    num_keys.push_back(stoi(substr));
  }

#if ASSERT_SORT
  vector<KEY_TYPE> correct;
#endif
  int num_of_lists = num_keys.size();
  IteratorWithModel<KEY_TYPE> **iterators = new IteratorWithModel<KEY_TYPE> *[num_of_lists];
  int total_num_of_keys = 0;
  for (int i = 0; i < num_of_lists; i++) {
    ModelBuilder<KEY_TYPE> *m = new PLRModelBuilder<KEY_TYPE>();
    IteratorBuilder<KEY_TYPE> *iterator_builder;
    if (FLAGS_disk_backed) {
      std::string fileName = "./DB/" + to_str(i + 1) + ".txt";
      std::cout << fileName << std::endl;
      iterator_builder = new IntDiskBuilder<KEY_TYPE>(
          fileName.c_str(), BUFFER_SIZE, "hello");
    } else {
      iterator_builder = new IntArrayBuilder<KEY_TYPE>(num_keys[i], "hello");
    }
    IteratorWithModelBuilder<KEY_TYPE> *builder =
        new IteratorWithModelBuilder<KEY_TYPE>(iterator_builder, m);
    auto keys = generate_keys(num_keys[i], FLAGS_universe_size);
    for (int j = 0; j < num_keys[i]; j++) {
      iterator_builder->add(keys[j]);
#if ASSERT_SORT
      correct.push_back(keys[j]);
#endif
    }
    iterators[i] = builder->finish();
    total_num_of_keys += num_keys[i];
  }

  if (FLAGS_print_result) {
    for (int i = 0; i < num_of_lists; i++) {
      IteratorWithModel<KEY_TYPE> *iter = iterators[i];
      iter->seekToFirst();
      printf("List %d\n", i);
      while (iter->valid()) {
        std::string key_str = to_str(iter->key());
        printf("%d Key: %llu %s\n", i, iter->current_pos(), key_str.c_str());
        iter->next();
      }
    }
  }
#if ASSERT_SORT
  sort(correct.begin(), correct.end());
#endif

  Comparator<KEY_TYPE> *c = new IntComparator<KEY_TYPE>();
  IteratorBuilder<KEY_TYPE> *resultBuilder;
  if (FLAGS_disk_backed) {
    resultBuilder = new IntDiskBuilder<KEY_TYPE>(
        "./DB/result.txt", BUFFER_SIZE, "result");
  } else {
    resultBuilder = new IntArrayBuilder<KEY_TYPE>(total_num_of_keys, "0");
  }

  Iterator<KEY_TYPE> *result;
  auto merge_start = std::chrono::high_resolution_clock::now();
  switch (FLAGS_merge_mode) {
    case STANDARD_MERGE:
      result = StandardMerger<KEY_TYPE>::merge(iterators, num_of_lists, c, resultBuilder);
      break;
    case MERGE_WITH_MODEL:
      result =
        LearnedMerger<KEY_TYPE>::merge(iterators, num_of_lists, c, resultBuilder);
        break;
  }
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();
  if (FLAGS_print_result) {
    printf("Merged List: %d\n", result->valid());
    while (result->valid()) {
      std::string key_str = to_str(result->key());
      printf("%s\n", key_str.c_str());
      result->next();
    }
  }

#if ASSERT_SORT
  result->seekToFirst();
  auto correctIterator = correct.begin();
  while (result->valid()) {
    KEY_TYPE k1 = result->key();
    KEY_TYPE k2 = *correctIterator;
    assert(k1 == k2);
    result->next();
    correctIterator++;
  }
#endif

  float duration_sec = duration_ns / 1e9;
  printf("Merge duration: %.3lf s\n", duration_sec);
  std::cout << "Ok!" << std::endl;
}

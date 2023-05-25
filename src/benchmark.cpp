#include <bits/stdc++.h>

#include <cassert>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "config.h"
#include "learned_merge.h"
#include "learned_merge_trust_bounds.h"
#include "slice_array_iterator.h"
#include "slice_comparator.h"
#include "slice_file_iterator.h"
#include "standard_merge.h"

using namespace std;

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

vector<KEY_TYPE> generate_keys(uint64_t num_keys, KEY_TYPE universe) {
  std::random_device rd;  // a seed source for the random number engine
  std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
  std::uniform_int_distribution<KEY_TYPE> distrib(1, universe);
  vector<KEY_TYPE> keys;
  for (int i = 0; i < num_keys; i++) {
    KEY_TYPE key = distrib(gen);
    keys.push_back(key);
  }
  sort(keys.begin(), keys.end());
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
  for (int i = 1; i < argc; i++) {
    double m;
    long long n;
    char junk;
    if (sscanf(argv[i], "--num_of_lists=%lld%c", &n, &junk) == 1) {
      FLAGS_num_of_lists = n;
    } else if (is_flag(argv[i], "--num_keys=")) {
      FLAGS_num_keys = argv[i] + strlen("--num_keys=");
    } else if (sscanf(argv[i], "--key_size_bytes=%lld%c", &n, &junk) == 1) {
      FLAGS_key_size_bytes = n;
    } else if (sscanf(argv[i], "--universe_log=%lld%c", &n, &junk) == 1) {
      FLAGS_universe_size = 1;
      for (int i = 0; i < n; i++) {
        FLAGS_universe_size = FLAGS_universe_size << 1;
      }
    } else if (sscanf(argv[i], "--use_disk=%lld%c", &n, &junk) == 1) {
      FLAGS_disk_backed = n;
    } else if (sscanf(argv[i], "--print_result=%lld%c", &n, &junk) == 1) {
      FLAGS_print_result = n;
    } else {
      printf("WARNING: unrecognized flag %s\n", argv[i]);
    }
  }

  if (FLAGS_disk_backed) {
    system("mkdir -p DB");
  }

#if !USE_STRING_KEYS
  if (FLAGS_key_size_bytes != sizeof(KEY_TYPE)) {
    abort();
  }
#endif

  std::cout << "Universe Size: " << to_str(FLAGS_universe_size) << std::endl;

  vector<int> num_keys;
  stringstream ss(FLAGS_num_keys);

#if ASSERT_SORT
  vector<KEY_TYPE> correct;
#endif

  while (ss.good()) {
    string substr;
    getline(ss, substr, ',');
    num_keys.push_back(stoi(substr));
  }
  int num_of_lists = num_keys.size();
  Iterator<Slice> **iterators = new Iterator<Slice> *[num_of_lists];
  int total_num_of_keys = 0;
  for (int i = 0; i < num_of_lists; i++) {
    IteratorBuilder<Slice> *builder;
    if (FLAGS_disk_backed) {
      std::string fileName = "./DB/" + to_str(i) + ".txt";
      std::cout << fileName << std::endl;
      builder = new FixedSizeSliceFileIteratorBuilder(
          fileName.c_str(), BUFFER_SIZE, FLAGS_key_size_bytes, i);
    } else {
      builder = new SliceArrayBuilder(num_keys[i], FLAGS_key_size_bytes, i);
    }
    auto keys = generate_keys(num_keys[i], FLAGS_universe_size);
    for (int j = 0; j < num_keys[i]; j++) {
#if ASSERT_SORT
      correct.push_back(keys[j]);
#endif
#if USE_STRING_KEYS
      std::string key = to_fixed_size_key(keys[j], FLAGS_key_size_bytes);
      builder->add(Slice(key.c_str(), FLAGS_key_size_bytes));
#else
      builder->add(Slice((char *)(&keys[j]), FLAGS_key_size_bytes));
#endif
    }
    iterators[i] = builder->finish();
    total_num_of_keys += num_keys[i];
  }

  if (FLAGS_print_result) {
    for (int i = 0; i < num_of_lists; i++) {
      Iterator<Slice> *iter = iterators[i];
      iter->seekToFirst();
      while (iter->valid()) {
#if USE_STRING_KEYS
        for (int i = 0; i < FLAGS_key_size_bytes; i++) {
          printf("%c", iter->key().data_[i]);
        }
        printf("\n");
#else
        KEY_TYPE *k = (KEY_TYPE *)iter->key().data_;
        printf("%d Key: %s\n", i, to_str(*k).c_str());
#endif
        iter->next();
      }
    }
  }

#if ASSERT_SORT
  sort(correct.begin(), correct.end());
#endif

  Comparator<Slice> *c = new SliceComparator();
  IteratorBuilder<Slice> *resultBuilder;
  if (FLAGS_disk_backed) {
    resultBuilder = new FixedSizeSliceFileIteratorBuilder(
        "./DB/result.txt", BUFFER_SIZE, FLAGS_key_size_bytes, 0);
  } else {
    resultBuilder =
        new SliceArrayBuilder(total_num_of_keys, FLAGS_key_size_bytes, 0);
  }

  Iterator<Slice> *result;
  auto merge_start = std::chrono::high_resolution_clock::now();
#if LEARNED_MERGE && !TRUST_ERROR_BOUNDS
  result =
      LearnedMerger<Slice>::merge(iterators, num_of_lists, c, resultBuilder);
#elif LEARNED_MERGE && TRUST_ERROR_BOUNDS
  result = LearnedMergerTrustBounds<Slice>::merge(iterators, num_of_lists, c,
                                                  resultBuilder);
#else
  result = StandardMerger::merge(iterators, num_of_lists, c, resultBuilder);
#endif
  auto merge_end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      merge_end - merge_start)
                      .count();

  if (FLAGS_print_result) {
    while (result->valid()) {
      Slice k = result->key();
#if USE_STRING_KEYS
      for (int i = 0; i < FLAGS_key_size_bytes; i++) {
        printf("%c", k.data_[i]);
      }
      printf("\n");
#else
      KEY_TYPE *val = (KEY_TYPE *)result->key().data_;
      printf("%s\n", to_str(*val).c_str());
#endif
      result->next();
    }
  }

  std::cout << "Merge duration: " << duration << " ns" << std::endl;
  std::cout << "Ok!" << std::endl;
}

#include <bits/stdc++.h>
#include <openssl/rand.h>

#include <cassert>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "slice_array_iterator.h"

#include "iterator.h"
#include "slice.h"
#include "config.h"
#include "learned_merge.h"
#include "learned_merge_trust_bounds.h"
#include "slice_array_iterator.h"
#include "slice_comparator.h"
#include "slice_file_iterator.h"
#include "standard_merge.h"
#include "plr_model.h"
#include "pgm_index.h"
#include "binary_search.h"
#include "model.h"

using namespace std;

#if USE_INT_128 && !USE_STRING_KEYS
static int FLAGS_key_size_bytes = 16;
#elif !USE_STRING_KEYS
static int FLAGS_key_size_bytes = 8;
#else
static int FLAGS_key_size_bytes = 20;
#endif

static const int BUFFER_SIZE = 1000;
static int FLAGS_num_of_lists = 2;
static KEY_TYPE FLAGS_universe_size = 2000000000000000;
static bool FLAGS_disk_backed = false;
static bool FLAGS_print_result = false;
static const char *FLAGS_num_keys = "100,100";
static const char *FLAGS_DB_dir = "./DB";

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
  for (uint64_t i = 0; i < bytes_to_alloc; i+=sizeof(universe)) {
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
  
  IteratorWithModel<Slice> **iterators = new IteratorWithModel<Slice> *[num_of_lists];
  int total_num_of_keys = 0;
  size_t segmentCount=0;
  size_t training_duration_ns=0;
  for (int i = 0; i < num_of_lists; i++) {
    IteratorWithModelBuilder<Slice> *builder;
    IteratorBuilder<Slice> *iterator;
    if (FLAGS_disk_backed) {
      std::string fileName = "./DB/" + to_str(i + 1) + ".txt";
      std::cout << fileName << std::endl;
      iterator = new FixedSizeSliceFileIteratorBuilder(fileName.c_str(), BUFFER_SIZE, FLAGS_key_size_bytes, i);
      
    } else {
      iterator = new SliceArrayBuilder(num_keys[i], FLAGS_key_size_bytes, i);
    }
    ModelBuilder<Slice> *m;
    #if USE_PLR_MODEL
    m = new PLRModelBuilder();
    #endif
    #if USE_PGM_INDEX
    m = new PGMIndexBuilder(num_keys[i]);
    #endif
    #if USE_BINARY_SEARCH
    m = new BinarySearchBuilder(num_keys[i]);
    #endif
    builder = new IteratorWithModelBuilder(iterator, m);
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
    training_duration_ns = builder->training_time_;
    segmentCount += iterators[i]->getNumberOfSegments();
    total_num_of_keys += num_keys[i];
  }
  
  
  float training_duration_sec = training_duration_ns / 1e9;
  printf("Training duration: %.3lf s\n", training_duration_sec);
  printf("Number of segments:%ld\n", segmentCount);
  std::cout << "Training done" << std::endl;

  if (FLAGS_print_result) {
    for (int i = 0; i < num_of_lists; i++) {
      Iterator<Slice> *iter = iterators[i];
      iter->seekToFirst();
      printf("List %d\n", i);
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
#if TRAIN_RESULT && LEARNED_MERGE
    resultBuilder = new FixedSizeSliceFileIteratorWithModelBuilder(
        "./DB/result.txt", BUFFER_SIZE, FLAGS_key_size_bytes, 0);
#else
    resultBuilder = new FixedSizeSliceFileIteratorBuilder(
        "./DB/result.txt", BUFFER_SIZE, FLAGS_key_size_bytes, 0);
#endif
  } else {
#if TRAIN_RESULT && LEARNED_MERGE
    resultBuilder = new SliceArrayWithModelBuilder(total_num_of_keys,
                                                   FLAGS_key_size_bytes, 0);
#else
    resultBuilder =
        new SliceArrayBuilder(total_num_of_keys, FLAGS_key_size_bytes, 0);
#endif
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
  auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         merge_end - merge_start)
                         .count();

  result->seekToFirst();
//   #if ASSERT_SORT
//   for(int i=0; i< correct.size();i++) {
//     #if USE_STRING_KEYS
//       std::string key = to_fixed_size_key(correct[i], FLAGS_key_size_bytes);
//       Slice k = Slice(key.c_str(), FLAGS_key_size_bytes);
// #else
//       Slice k = Slice((char *)(&correct[i]), FLAGS_key_size_bytes);
// #endif
//     if(c->compare(k, result->peek(i)) != 0) {
//       std::cout<<"Assert sort failed"<<std::endl;
//       abort();
//     }
//   }
//   #endif
  if (FLAGS_print_result) {
    printf("Merged List: %d\n", result->valid());
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
#if ASSERT_SORT
  result->seekToFirst();
  auto correctIterator = correct.begin();
  while (result->valid()) {
    KEY_TYPE *k1 = (KEY_TYPE *)result->key().data_;
    KEY_TYPE k2 = *correctIterator;
    assert(*k1 == k2);
    result->next();
    correctIterator++;
  }
#endif

  float duration_sec = duration_ns / 1e9;
  printf("Merge duration: %.3lf s\n", duration_sec);
  std::cout << "Ok!" << std::endl;
}

#include <bits/stdc++.h>

#include <cassert>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "int_iterator.h"
#include "learned_merge.h"
#include "learned_merge_trust_bounds.h"
#include "standard_merge.h"

using namespace std;

static const int BUFFER_SIZE = 1000;
static int FLAGS_num_of_lists = 2;
static uint64_t FLAGS_universe_size = 2000000000000000;
static int FLAGS_key_size_bytes = 16;
static bool FLAGS_disk_backed = false;
static bool FLAGS_print_result = false;
static const char *FLAGS_num_keys = "10,10";
static const char *FLAGS_DB_dir = "./DB";

vector<uint64_t> generate_keys(uint64_t num_keys, uint64_t universe) {
  std::random_device rd;  // a seed source for the random number engine
  std::mt19937 gen(rd()); // mersenne_twister_engine seeded with rd()
  std::uniform_int_distribution<uint64_t> distrib(1, universe);
  vector<uint64_t> keys;
  for (int i = 0; i < num_keys; i++) {
    uint64_t key = distrib(gen);
    keys.push_back(key);
  }
  sort(keys.begin(), keys.end());
  return keys;
}

bool is_flag(const char *arg, const char *flag) {
  return strncmp(arg, flag, strlen(flag)) == 0;
}

int main(int argc, char **argv) {
  char db_dir[100];
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
    } else if (sscanf(argv[i], "--print_result=%lld%c", &n, &junk) == 1) {
      FLAGS_print_result = n;
    } else {
      printf("WARNING: unrecognized flag %s\n", argv[i]);
    }
  }

  if (FLAGS_disk_backed) {
    system("mkdir -p DB");
  }

  vector<int> num_keys;
  stringstream ss(FLAGS_num_keys);

  while (ss.good()) {
    string substr;
    getline(ss, substr, ',');
    num_keys.push_back(stoi(substr));
  }

  int num_of_lists = num_keys.size();
  Iterator<uint64_t> **iterators = new Iterator<uint64_t> *[num_of_lists];
  int total_num_of_keys = 0;
  for (int i = 0; i < num_of_lists; i++) {
    IteratorBuilder<uint64_t> *builder =
        new IntArrayIteratorBuilder<uint64_t>(num_keys[i]);
    auto keys = generate_keys(num_keys[i], FLAGS_universe_size);
    for (int j = 0; j < num_keys[i]; j++) {
      builder->add(keys[j]);
    }
    iterators[i] = builder->finish();
    total_num_of_keys += num_keys[i];
  }

  Comparator<uint64_t> *c = new IntComparator<uint64_t>();
  IteratorBuilder<uint64_t> *resultBuilder;
  resultBuilder = new IntArrayIteratorBuilder<uint64_t>(total_num_of_keys);

  Iterator<uint64_t> *result;
  auto merge_start = std::chrono::high_resolution_clock::now();
#if LEARNED_MERGE && !TRUST_ERROR_BOUNDS
  result =
      LearnedMerger<uint64_t>::merge(iterators, num_of_lists, c, resultBuilder);
#elif LEARNED_MERGE && TRUST_ERROR_BOUNDS
  result = LearnedMergerTrustBounds<uint64_t>::merge(iterators, num_of_lists, c,
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
      printf("%lu\n", result->key());
      result->next();
    }
  }
  std::cout << "Merge duration: " << duration << " ns" << std::endl;
  std::cout << "Ok!" << std::endl;
}

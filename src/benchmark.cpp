#include "learned_merge.h"
#include "slice_array_iterator.h"
#include "slice_comparator.h"
#include "standard_merge.h"
#include <bits/stdc++.h>
#include <cassert>
#include <cstring>
#include <string>
#include <vector>

using namespace std;

static int FLAGS_num_of_lists = 2;
static int FLAGS_key_size_bytes = 10;
static const char *FLAGS_merge_type = "learned";
static const char *FLAGS_num_of_items_per_list = "10, 10";

string generate_key(int key_value, int key_size) {
  string key = to_string(key_value);
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
    } else if (is_flag(argv[i], "--merge_type=")) {
      FLAGS_merge_type = argv[i] + strlen("--merge_type=");
    } else if (is_flag(argv[i], "--num_of_items_per_list=")) {
      FLAGS_num_of_items_per_list =
          argv[i] + strlen("--num_of_items_per_list=");
    } else if (sscanf(argv[i], "--key_size_bytes=%lld%c", &n, &junk) == 1) {
      FLAGS_key_size_bytes = n;
    } else {
      printf("WARNING: unrecognized flag %s\n", argv[i]);
    }
  }

  vector<int> num_of_items_per_list;
  stringstream ss(FLAGS_num_of_items_per_list);

  while (ss.good()) {
    string substr;
    getline(ss, substr, ',');
    num_of_items_per_list.push_back(stoi(substr));
  }
  int num_of_lists = num_of_items_per_list.size();
  Iterator<Slice> **iterators = new Iterator<Slice> *[num_of_lists];
  int total_num_of_keys = 0;
  for (int i = 0; i < num_of_lists; i++) {
    SliceArrayBuilder *builder =
        new SliceArrayBuilder(num_of_items_per_list[i], FLAGS_key_size_bytes);
    for (int j = 0; j < num_of_items_per_list[i]; j++) {
      std::string key = generate_key(2 * j, FLAGS_key_size_bytes);
      builder->add(Slice(key.c_str(), FLAGS_key_size_bytes));
    }
    iterators[i] = builder->finish();
    total_num_of_keys += num_of_items_per_list[i];
  }

  Comparator<Slice> *c = new SliceComparator();

  SliceArrayBuilder *resultBuilder =
      new SliceArrayBuilder(total_num_of_keys, FLAGS_key_size_bytes);
  Iterator<Slice> *result;
  if (FLAGS_merge_type == "standard") {
    result = StandardMerger::merge(iterators, num_of_lists, c, resultBuilder);
  } else {
    result =
        LearnedMerger<Slice>::merge(iterators, num_of_lists, c, resultBuilder);
  }
  while (result->valid()) {
    cout << result->key().toString() << endl;
    result->next();
  }
  std::cout << "Ok!" << std::endl;
}

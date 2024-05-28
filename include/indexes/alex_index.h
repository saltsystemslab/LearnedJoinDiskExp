#ifndef LEARNEDINDEXMERGE_ALEX_INDEX_H
#define LEARNEDINDEXMERGE_ALEX_INDEX_H

#if USE_ALEX

#include "comparator.h"
#include "index.h"
#include "key_value_slice.h"
#include "alex.h"
#include <algorithm>
#include <fstream>
#include <map>
#include <vector>

namespace li_merge {


class AlexIndex: public Index<KVSlice> {
public:
  AlexIndex(alex::Alex<uint64_t, uint64_t> *alex) {
    this->alex = alex;
  }

  Bounds getPositionBoundsRA(const KVSlice &t) override {
    uint64_t v;
    int tmp;
    uint64_t *key = (uint64_t *)(t.data());
    bool found = alex->get_leaf_disk(*key, &v, &tmp, &tmp, &tmp);
    if (found) {
      return Bounds{1, 1, 1};
    } else {
      return Bounds{0, 0, 0};
    }
  }

  // Again not the cleanest implementation. 
  // This interface assumes last-mile search is handled outside.
  // Alex handles last-mile. So we just set 1 if item is found, 0 otherwise.
  // Ideally, this would not use this interface but would be its own interface.
  Bounds getPositionBounds(const KVSlice &t) override {
    uint64_t v;
    int tmp;
    uint64_t *key = (uint64_t *)(t.data());
    bool found = alex->get_leaf_disk(*key, &v, &tmp, &tmp, &tmp);
    if (found) {
      return Bounds{1, 1, 1};
    } else {
      return Bounds{0, 0, 0};
    }
  }

  uint64_t sizeInBytes() override { return alex->get_memory_size(); }
  uint64_t getMaxError() override { return 0; }
  bool isErrorPageAligned() override { return true; }

private:
  alex::Alex<uint64_t, uint64_t> *alex;
};

class AlexIndexBuilder : public IndexBuilder<KVSlice> {
public:
  AlexIndexBuilder(std::string indexfp) {
    std::string index_path = indexfp + "_index";
    std::string data_path = indexfp + "_data";

    char *data_file_name = new char[data_path.size()+1];
    memset(data_file_name, 0, data_path.size() + 1);
    memcpy(data_file_name, data_path.c_str(),data_path.size());

    char *index_file_name = new char[index_path.size()+1];
    memset(index_file_name, 0, index_path.size() + 1);
    memcpy(index_file_name, index_path.c_str(),index_path.size());

    alex = new alex::Alex<uint64_t, uint64_t>(LEAF_DISK, true, data_file_name, index_file_name);
    num_elts_ = 0;
  }
  void add(const KVSlice &t) override {
    uint64_t *key = (uint64_t *)(t.data());
    std::pair<uint64_t, uint64_t> p;
    p.first = *key;
    p.second = num_elts_;
    num_elts_++;
    elts_.push_back(p);
  }
  Index<KVSlice> *build() override {
    alex->bulk_load(&elts_[0], num_elts_);
    return new AlexIndex(alex);
  }
  void backToFile(std::string filename) override { 
    // We recreate the index everytime.
  }

private:
  uint64_t num_elts_;
  std::vector<std::pair<uint64_t, uint64_t>> elts_;
  alex::Alex<uint64_t, uint64_t> *alex;
};

} // namespace li_merge

#endif
#endif

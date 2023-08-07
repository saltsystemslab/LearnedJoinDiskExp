#ifndef BETREE_H
#define BETREE_H

#include "betree.hpp"
#include "index.h"
#include "key_value_slice.h"
#include <filesystem>

namespace li_merge {

class BeTreeIndex : public Index<KVSlice> {
public:
  BeTreeIndex(betree<uint64_t, uint64_t> *betree, uint64_t start_index=0, uint64_t end_index=-1)
  : betree_(betree), start_index_(start_index), end_index_(end_index) {} 
  uint64_t getApproxPosition(const KVSlice &t) { 
    uint64_t *key = (uint64_t *)(t.data());
    auto iterator = betree_->lower_bound(*key);
    uint64_t pos = iterator.second;
    pos = std::clamp(pos, start_index_, end_index_);
    return pos - start_index_;
  };
  uint64_t getApproxLowerBoundPosition(const KVSlice &t) { 
    return getApproxPosition(t);
  };
  Bounds getPositionBounds(const KVSlice &t) { abort(); };
  uint64_t getApproxPositionMonotoneAccess(const KVSlice &t) { 
    return getApproxPosition(t);
  };
  uint64_t getApproxLowerBoundPositionMonotoneAccess(const KVSlice &t) {
    return getApproxPosition(t);
  };
  Bounds getPositionBoundsMonotoneAccess(const KVSlice &t) { 
    abort();
  };
  void resetMonotoneAccess() { abort(); };
  uint64_t size_in_bytes() { return 0; };
  Index<KVSlice> *getIndexForSubrange(uint64_t start, uint64_t end) {
    return new BeTreeIndex(betree_, start, end);
  };

private:
  // TODO: Implement comparator for betree.
  betree<uint64_t, uint64_t> *betree_;
  uint64_t start_index_;
  uint64_t end_index_;
};

class BeTreeIndexBuilder : public IndexBuilder<KVSlice> {
public:
    BeTreeIndexBuilder(std::string backing_store_dir, uint64_t cache_size, uint64_t node_size, uint64_t flush_size) {
        std::filesystem::create_directory(backing_store_dir);
        ofpobs_ = new one_file_per_object_backing_store(backing_store_dir.c_str());
        sspace_ = new swap_space(ofpobs_, cache_size/* cache size, #of betree nodes. */);
        betree_ = new betree<uint64_t, uint64_t>(sspace_, node_size /* Keys per betree node*/, flush_size);
        num_elts_ = 0;
    }
  void add(const KVSlice &t) override {
    uint64_t *key = (uint64_t *)(t.data());
    num_elts_++;
    betree_->insert(*key, num_elts_);
  }
  Index<KVSlice> *build() { 
    betree_->insert(-1, num_elts_ + 1);
    return new BeTreeIndex(betree_); 
  }
private:
  betree<uint64_t, uint64_t> *betree_;
  one_file_per_object_backing_store *ofpobs_;
  swap_space *sspace_;
  uint64_t num_elts_;
};

} // namespace li_merge

#endif
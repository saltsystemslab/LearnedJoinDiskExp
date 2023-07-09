#ifndef SLICE_PGM_MODEL_H
#define SLICE_PGM_MODEL_H

#include "iterator.h"
#include "model.h"
#include "plr.h"
#include "slice.h"
#include "pgm/pgm_index.hpp"

#include <iostream>
#include <map>
using namespace std;

class SliceStdTreeModel: public Model<Slice> {
  public:
    SliceStdTreeModel(std::map<std::string, uint64_t> *index): index_(index), start_offset_(0), num_keys_(index->size()) {}
    SliceStdTreeModel(std::map<std::string, uint64_t> *index, uint64_t start_offset, uint64_t num_keys)
      : index_(index), num_keys_(num_keys), start_offset_(start_offset) {}
    uint64_t guessPositionMonotone(Slice target_slice_key) override {
      return guessPosition(target_slice_key);
    };
    uint64_t guessPosition(Slice target_slice_key) override {
      std::string q(target_slice_key.data_, target_slice_key.size_);
      auto entry = index_->lower_bound(q);;
      if (entry == index_->end()) {
        return num_keys_ - 1;
      }
      return entry->second - start_offset_;
    }

  double getMaxError() override { return 1; }

  uint64_t size_bytes() override {
    return index_->size() * (index_->begin()->first.size());
  }

 SliceStdTreeModel *get_model_for_subrange(uint64_t start, uint64_t end) override {
    return new SliceStdTreeModel(index_, start, end-start);
  }

  private:
    std::map<std::string, uint64_t> *index_;
    uint64_t start_offset_;
    uint64_t num_keys_;
};

class SlicePGMModel : public Model<Slice> {
public:
  SlicePGMModel(pgm::PGMIndex<PLR_SEGMENT_POINT, 64> *pgm_index, uint64_t num_keys, 
    SliceToPlrPointConverter *slice_to_point_converter)
      : pgm_index_(pgm_index), num_keys_(num_keys),
        start_offset_(0), converter_(slice_to_point_converter) {
  }

  SlicePGMModel(pgm::PGMIndex<PLR_SEGMENT_POINT, 64> *pgm_index, uint64_t num_keys, uint64_t start_offset,
    SliceToPlrPointConverter *slice_to_point_converter)
      : pgm_index_(pgm_index), num_keys_(num_keys),
        start_offset_(start_offset), converter_(slice_to_point_converter) {
    }

  uint64_t guessPositionMonotone(Slice target_slice_key) override {
    PLR_SEGMENT_POINT point = converter_->to_plr_point(target_slice_key);
    auto pos = pgm_index_->search(point).pos;
    if (pos > start_offset_) {
      return pos - start_offset_;
    }
    return 0;
  };

  uint64_t guessPosition(Slice target_slice_key) override {
    PLR_SEGMENT_POINT point = converter_->to_plr_point(target_slice_key);
    auto pos = pgm_index_->search(point).pos;
    if (pos > start_offset_) {
      return pos - start_offset_;
    }
    return 0;
  }

  SlicePGMModel *get_model_for_subrange(uint64_t start, uint64_t end) override {
    return new SlicePGMModel(pgm_index_, end-start, start, converter_);
  }

  double getMaxError() override { return 64; }

  uint64_t size_bytes() override {
    return pgm_index_->size_in_bytes();
  }

private:
  pgm::PGMIndex<PLR_SEGMENT_POINT, 64> *pgm_index_;
  SliceToPlrPointConverter *converter_;
  uint64_t start_offset_;
  uint64_t num_keys_;
};

class PGMModelBuilder: public ModelBuilder<Slice> {
public:
  PGMModelBuilder(SliceToPlrPointConverter *converter):
    converter_(converter) {}

  void add(const Slice &slice_key) override {
    PLR_SEGMENT_POINT t = converter_->to_plr_point(slice_key);
    points.push_back(t);
  }
  SlicePGMModel *finish() override {
    return new SlicePGMModel(new pgm::PGMIndex(points), points.size(), converter_);
  }

private:
  vector<PLR_SEGMENT_POINT> points;
  SliceToPlrPointConverter *converter_;
};

class StdTreeModelBuilder: public ModelBuilder<Slice> {
public:
  StdTreeModelBuilder() {
    cur_idx_ = 0;
    tree_ = new std::map<std::string, uint64_t>();
  }

  void add(const Slice &slice_key) override {
    std::string entry(slice_key.data_, slice_key.size_);
    tree_->insert(std::pair<std::string, uint64_t>(entry, cur_idx_++));
  }
  SliceStdTreeModel *finish() override {
    return new SliceStdTreeModel(tree_);
  }

private:
  uint64_t cur_idx_;
  std::map<std::string, uint64_t> *tree_;
};

#endif

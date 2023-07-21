#ifndef LEARNEDINDEXMERGE_GREEDY_PLR_H
#define LEARNEDINDEXMERGE_GREEDY_PLR_H

#include "index.h"
#include "key_to_point.h"
#include "key_value_slice.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <iostream>
#include <string>
#include <vector>

#include "config.h"

namespace li_merge {
namespace greedy_plr {

// Code modified from https://github.com/RyanMarcus/plr

struct point {
  double x;
  double y;

  point() = default;
  point(double x, double y) : x(x), y(y) {}
};

struct line {
  double a;
  double b;
};

class Segment {
public:
  Segment() {}
  Segment(POINT_FLOAT_TYPE _x, POINT_FLOAT_TYPE _last, double _k, double _b)
      : x(_x), last(_last), k(_k), b(_b) {}
  POINT_FLOAT_TYPE x;
  POINT_FLOAT_TYPE last;
  double k;
  double b;
};

class PLRModel {
public:
  PLRModel() { numKeys_ = 0; }
  PLRModel(std::vector<Segment> lineSegments, int numKeys, double gamma)
      : lineSegments_(lineSegments), numKeys_(numKeys), gamma_(gamma) {}
  std::vector<Segment> lineSegments_;
  uint64_t numKeys_;
  double gamma_;
};

double get_slope(struct point p1, struct point p2);
struct line get_line(struct point p1, struct point p2);
struct point get_intersetction(struct line l1, struct line l2);

bool is_above(struct point pt, struct line l);
bool is_below(struct point pt, struct line l);

struct point get_upper_bound(struct point pt, double gamma);
struct point get_lower_bound(struct point pt, double gamma);

class PLRBuilder {
private:
  std::string state;
  double gamma;
  struct point last_pt;
  struct point s0;
  struct point s1;
  struct line rho_lower;
  struct line rho_upper;
  struct point sint;
  POINT_FLOAT_TYPE prev_key;
  POINT_FLOAT_TYPE idx;
  std::vector<Segment> segments;

  void setup();
  Segment current_segment();
  Segment process__(struct point pt);
  Segment process(const struct point &pt);
  Segment finish();

public:
  PLRBuilder(double gamma);
  void processKey(const POINT_FLOAT_TYPE key);
  PLRModel *finishTraining();
  void reset();
};

double get_slope(struct point p1, struct point p2) {
  return (p2.y - p1.y) / (p2.x - p1.x);
}

struct line get_line(struct point p1, struct point p2) {
  double a = get_slope(p1, p2);
  double b = -a * p1.x + p1.y;
  struct line l {
    .a = a, .b = b
  };
  return l;
}

struct point get_intersetction(struct line l1, struct line l2) {
  double a = l1.a;
  double b = l2.a;
  double c = l1.b;
  double d = l2.b;
  struct point p {
    (d - c) / (a - b), (a * d - b * c) / (a - b)
  };
  return p;
}

bool is_above(struct point pt, struct line l) {
  return pt.y > l.a * pt.x + l.b;
}

bool is_below(struct point pt, struct line l) {
  return pt.y < l.a * pt.x + l.b;
}

struct point get_upper_bound(struct point pt, double gamma) {
  struct point p {
    pt.x, pt.y + gamma
  };
  return p;
}

struct point get_lower_bound(struct point pt, double gamma) {
  struct point p {
    pt.x, pt.y - gamma
  };
  return p;
}

PLRBuilder::PLRBuilder(double gamma) {
  this->state = "need2";
  this->gamma = gamma;
  this->idx = 0;
#if TRACK_PLR_TRAIN_TIME
  this->training_time = 0;
#endif
}

void PLRBuilder::reset() {
  this->segments.clear();
  this->idx = 0;
  this->state = "need2";
}

Segment PLRBuilder::process(const struct point &pt) {
  Segment s = {0, 0, 0, 0};
  this->last_pt = pt;
  if (this->state.compare("need2") == 0) {
    this->s0 = pt;
    this->state = "need1";
  } else if (this->state.compare("need1") == 0) {
    this->s1 = pt;
    setup();
    this->state = "ready";
  } else if (this->state.compare("ready") == 0) {
    s = process__(pt);
  } else {
    // impossible
    std::cout << "ERROR in process" << std::endl;
  }

  return s;
}

void PLRBuilder::setup() {
  this->rho_lower = get_line(get_upper_bound(this->s0, this->gamma),
                             get_lower_bound(this->s1, this->gamma));
  this->rho_upper = get_line(get_lower_bound(this->s0, this->gamma),
                             get_upper_bound(this->s1, this->gamma));
  this->sint = get_intersetction(this->rho_upper, this->rho_lower);
}

Segment PLRBuilder::current_segment() {
  POINT_FLOAT_TYPE segment_start = this->s0.x;
  POINT_FLOAT_TYPE segment_stop = this->last_pt.x;
  double avg_slope = (this->rho_lower.a + this->rho_upper.a) / 2.0;
  double intercept = -avg_slope * this->sint.x + this->sint.y;
  Segment s = {segment_start, segment_stop, avg_slope, intercept};
  return s;
}

Segment PLRBuilder::process__(struct point pt) {
  if (!(is_above(pt, this->rho_lower) && is_below(pt, this->rho_upper))) {
    // new point out of error bounds
    Segment prev_segment = current_segment();
    this->s0 = pt;
    this->state = "need1";
    return prev_segment;
  }

  struct point s_upper = get_upper_bound(pt, this->gamma);
  struct point s_lower = get_lower_bound(pt, this->gamma);
  if (is_below(s_upper, this->rho_upper)) {
    this->rho_upper = get_line(this->sint, s_upper);
  }
  if (is_above(s_lower, this->rho_lower)) {
    this->rho_lower = get_line(this->sint, s_lower);
  }
  Segment s = {0, 0, 0, 0};
  return s;
}

Segment PLRBuilder::finish() {
  Segment s = {0, 0, 0, 0};
  if (this->state.compare("need2") == 0) {
    this->state = "finished";
    return s;
  } else if (this->state.compare("need1") == 0) {
    this->state = "finished";
    s.x = this->s0.x;
    s.last = this->s0.x + 1;
    s.k = 0;
    s.b = this->s0.y;
    return s;
  } else if (this->state.compare("ready") == 0) {
    this->state = "finished";
    return current_segment();
  } else {
    std::cout << "ERROR in finish" << std::endl;
    return s;
  }
}

void PLRBuilder::processKey(POINT_FLOAT_TYPE key) {
  if (this->idx == 0 || (prev_key != key)) {
    Segment seg = this->process(point((double)key, idx));
    if (seg.x != 0 || seg.k != 0 || seg.b != 0) {
      this->segments.push_back(seg);
    }
  }
  prev_key = key;
  this->idx++;
}

PLRModel *PLRBuilder::finishTraining() {
  Segment last = this->finish();
  if (last.x != 0 || last.k != 0 || last.b != 0) {
    this->segments.push_back(last);
  }
  PLRModel *model = new PLRModel(this->segments, this->idx, this->gamma);
  return model;
}

} // namespace greedy_plr

template <class T> class GreedyPLRIndex : public Index<T> {
public:
  GreedyPLRIndex(greedy_plr::PLRModel *index, KeyToPointConverter<T> *converter)
      : greedy_plr_index_(index), converter_(converter),
        num_keys_(index->numKeys_), cur_segment_index_(0) {}

  uint64_t getApproxPosition(const T &t) override {
    POINT_FLOAT_TYPE target_key = converter_->toPoint(t);
    std::vector<greedy_plr::Segment> &segments =
        greedy_plr_index_->lineSegments_;
    int64_t left = 0, right = (int64_t)segments.size() - 1;
    while (left < right) {
      int64_t mid = (right + left + 1) / 2;
      if (target_key < segments[mid].x)
        right = mid - 1;
      else
        left = mid;
    }
    uint64_t result =
        std::ceil((target_key)*segments[left].k + segments[left].b);
#if 0
    if (left+1 < segments.size()) {
      result = std::min<uint64_t>(result, std::ceil(segments[left+1].b));
    }
#endif
    if (result < 0) {
      result = 0;
    }
    if (result >= num_keys_) {
      result = num_keys_ - 1;
    }
    return result;
  }
  uint64_t getApproxLowerBoundPosition(const KVSlice &t) override {
    uint64_t position = getApproxPosition(t);
    if (position >= greedy_plr_index_->gamma_) {
      position = position - greedy_plr_index_->gamma_;
    } else {
      position = 0;
    }
    return position;
  };
  Bounds getPositionBounds(const T &t) override {
    uint64_t approx_pos = getApproxPosition(t);
    if (approx_pos > greedy_plr_index_->gamma_) {
      approx_pos = approx_pos - 0;
    } else {
      approx_pos = 0;
    }
    return Bounds{approx_pos, approx_pos + greedy_plr_index_->gamma_,
                  approx_pos};
  }

  uint64_t getApproxPositionMonotoneAccess(const T &t) override {
    std::vector<greedy_plr::Segment> &segments =
        greedy_plr_index_->lineSegments_;
    POINT_FLOAT_TYPE target_key = converter_->toPoint(t);
    for (uint64_t i = cur_segment_index_; i < (uint64_t)segments.size(); i++) {
      if (segments[i].last > target_key) {
        cur_segment_index_ = i;
        uint64_t result = std::ceil(target_key * segments[i].k + segments[i].b);
#if 0
        if (i+1 < segments.size()) {
          result = std::min<uint64_t>(result, std::ceil(segments[i+1].b));
        }
#endif
        if (result >= num_keys_) {
          result = num_keys_ - 1;
        }
        return result;
      }
    }
    return num_keys_ - 1;
  }
  uint64_t getApproxLowerBoundPositionMonotoneAccess(const KVSlice &t) override {
    uint64_t position = getApproxPositionMonotoneAccess(t);
    if (position >= greedy_plr_index_->gamma_) {
      position = std::ceil(position - greedy_plr_index_->gamma_);
    } else {
      position = 0;
    }
    return position;
  };

  Bounds getPositionBoundsMonotoneAccess(const T &t) override {
    uint64_t approx_pos = getApproxPosition(t);
    if (approx_pos > greedy_plr_index_->gamma_) {
      approx_pos = approx_pos - 0;
    } else {
      approx_pos = 0;
    }
    return Bounds{approx_pos, approx_pos + greedy_plr_index_->gamma_,
                  approx_pos};
  }

  void resetMonotoneAccess() override {
    cur_segment_index_ = 0;
    last_point_ = 0;
  }

  uint64_t size_in_bytes() override {
    return greedy_plr_index_->lineSegments_.size() *
           sizeof(greedy_plr::Segment);
  }

private:
  uint64_t num_keys_;
  greedy_plr::PLRModel *greedy_plr_index_;
  KeyToPointConverter<T> *converter_;
  POINT_FLOAT_TYPE last_point_;
  uint64_t cur_segment_index_;
};

template <class T> class GreedyPLRIndexBuilder : public IndexBuilder<T> {
public:
  GreedyPLRIndexBuilder(double gamma, KeyToPointConverter<T> *converter)
      : converter_(converter), builder_(new greedy_plr::PLRBuilder(gamma)) {}
  void add(const T &t) override {
    builder_->processKey(converter_->toPoint(t));
  }
  Index<KVSlice> *build() override {
    return new GreedyPLRIndex<T>(builder_->finishTraining(), converter_);
  }

private:
  uint64_t num_keys_;
  KeyToPointConverter<T> *converter_;
  greedy_plr::PLRBuilder *builder_;
};

} // namespace li_merge
#endif

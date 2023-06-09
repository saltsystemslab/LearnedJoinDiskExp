#include "plr.h"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <string>
#include <vector>

using std::string;

// Code modified from https://github.com/RyanMarcus/plr

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

int counter = 0;

KEY_TYPE LdbKeyToInteger(const Slice &key) {
  const char *data = key.data_;
  size_t size = key.size_;
  KEY_TYPE num = 0;
  bool leading_zeros = true;

  for (int i = 0; i < size; ++i) {
    int temp = data[i];
    // TODO: Figure out where the extra bytes are coming from
    if (temp < '0' || temp > '9')
      break;
    if (leading_zeros && temp == '0')
      continue;
    leading_zeros = false;
    num = (num << 3) + (num << 1) + temp - 48;
  }
  return num;
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
  KEY_TYPE segment_start = this->s0.x;
  KEY_TYPE segment_stop = this->last_pt.x;
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

void PLRBuilder::processKey(KEY_TYPE key) {
#if TRACK_PLR_TRAIN_TIME
  auto train_start = std::chrono::high_resolution_clock::now();
#endif
  if (this->idx == 0 || (prev_key != key)) {
    Segment seg = this->process(point((double)key, idx));
    if (seg.x != 0 || seg.k != 0 || seg.b != 0) {
      this->segments.push_back(seg);
    }
  }
  prev_key = key;
  this->idx++;
#if TRACK_PLR_TRAIN_TIME
  auto train_end = std::chrono::high_resolution_clock::now();
  training_time += std::chrono::duration_cast<std::chrono::nanoseconds>(
                       train_end - train_start)
                       .count();
#endif
}

PLRModel *PLRBuilder::finishTraining() {
  Segment last = this->finish();
  if (last.x != 0 || last.k != 0 || last.b != 0) {
    this->segments.push_back(last);
  }
  PLRModel *model = new PLRModel(this->segments, this->idx);
#if TRACK_PLR_TRAIN_TIME
  std::cout << "PLR Training time: " << training_time << std::endl;
#endif

  return model;
}

#ifndef PLR_H_
#define PLR_H_

#include <deque>
#include <string>
#include <vector>

#include "config.h"
#include "slice.h"

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
  Segment(KEY_TYPE _x, KEY_TYPE _last, double _k, double _b)
      : x(_x), last(_last), k(_k), b(_b) {}
  KEY_TYPE x;
  KEY_TYPE last;
  double k;
  double b;
};

class PLRModel {
public:
  PLRModel() { numKeys_ = 0; }
  PLRModel(std::vector<Segment> lineSegments, int numKeys)
      : lineSegments_(lineSegments), numKeys_(numKeys) {}
  std::vector<Segment> lineSegments_;
  int numKeys_;
};

double get_slope(struct point p1, struct point p2);
struct line get_line(struct point p1, struct point p2);
struct point get_intersetction(struct line l1, struct line l2);

bool is_above(struct point pt, struct line l);
bool is_below(struct point pt, struct line l);

struct point get_upper_bound(struct point pt, double gamma);
struct point get_lower_bound(struct point pt, double gamma);

KEY_TYPE LdbKeyToInteger(const Slice &key);

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
  KEY_TYPE prev_key;
  KEY_TYPE idx;
  std::vector<Segment> segments;

  void setup();
  Segment current_segment();
  Segment process__(struct point pt);
  Segment process(const struct point &pt);
  Segment finish();
#if TRACK_PLR_TRAIN_TIME
  uint64_t training_time;
#endif

public:
  PLRBuilder(double gamma);
  void processKey(const KEY_TYPE key);
  PLRModel *finishTraining();
  void reset();
};
#endif
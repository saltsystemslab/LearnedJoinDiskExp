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
  Segment(PLR_SEGMENT_POINT _x, PLR_SEGMENT_POINT _last, double _k, double _b)
      : x(_x), last(_last), k(_k), b(_b) {}
  PLR_SEGMENT_POINT x;
  PLR_SEGMENT_POINT last;
  double k;
  double b;
};

class PLRModel {
public:
  PLRModel() { numKeys_ = 0; }
  PLRModel(std::vector<Segment> lineSegments, int numKeys, double gamma)
      : lineSegments_(lineSegments), numKeys_(numKeys), gamma_(gamma) {}
  std::vector<Segment> lineSegments_;
  int numKeys_;
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
  PLR_SEGMENT_POINT prev_key;
  PLR_SEGMENT_POINT idx;
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
  void processKey(const PLR_SEGMENT_POINT key);
  PLRModel *finishTraining();
  void reset();
};
#endif

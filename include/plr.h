#ifndef PLR_H_
#define PLR_H_

#include <deque>
#include <string>
#include <vector>

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
  Segment(uint64_t _x, uint64_t _last, double _k, double _b)
      : x(_x), last(_last), k(_k), b(_b) {}
  uint64_t x;
  uint64_t last;
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

uint64_t LdbKeyToInteger(const std::string &str);

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
  uint64_t prev_key;
  uint64_t idx;
  std::vector<Segment> segments;

  void setup();
  Segment current_segment();
  Segment process__(struct point pt);
  Segment process(const struct point &pt);
  Segment finish();

public:
  PLRBuilder(double gamma);
  void processKey(uint64_t key);
  PLRModel *finishTraining();
  void reset();
};
#endif
#include "util/thread_random.hh"

namespace tuskflow {

namespace {
int g_tracking_threshold = 40;
}  // namespace

Xoroshiro128Plus& tl_rng() {
  thread_local Xoroshiro128Plus rng;
  return rng;
}

bool track_access_draw() {
  return static_cast<int>(tl_rng().next() % 100) < g_tracking_threshold;
}

void set_tracking_threshold(int percent) { g_tracking_threshold = percent; }

}  // namespace tuskflow

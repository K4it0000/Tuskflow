#pragma once

// ThreadLocalRandom of the original: one generator per thread.

#include "util/random.hh"

namespace tuskflow {

Xoroshiro128Plus& tl_rng();

// GraphStorage.trackAccesses(): ThreadLocalRandom.nextInt(0, 100) <
// TRACKING_THRESHOLD.  Used by storage/graph_storage.hh.
bool track_access_draw();
void set_tracking_threshold(int percent);

}  // namespace tuskflow

#pragma once

#include <atomic>
#include <cstdint>

namespace tuskflow {

// PaddedAtomicLong of the original: a counter on its own cache line.
struct alignas(64) PaddedCounter {
  std::atomic<uint64_t> v{0};
};

}  // namespace tuskflow

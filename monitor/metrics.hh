#pragma once

// Metrics of the original: committed transactions and end-to-end latency per
// worker.

#include <atomic>
#include <cstdint>
#include <vector>

#include "util/padded_counter.hh"

namespace tuskflow {

struct LatencySample {
  double end_ms;
  double latency_ms;
};

class Metrics {
public:
  explicit Metrics(size_t workers) : committed_(workers), latency_(workers) {}
  void add_committed(size_t w) {
    committed_[w].v.fetch_add(1, std::memory_order_relaxed);
  }
  void add_latency(size_t w, double end_ms, double latency_ms) {
    if (latency_ms == 0) return;  // LatencyMetric skips zero measurements
    latency_[w].push_back({end_ms, latency_ms});
  }
  uint64_t total_committed() const {
    uint64_t s = 0;
    for (const auto& c : committed_) s += c.v.load(std::memory_order_relaxed);
    return s;
  }
  std::vector<LatencySample> all_latencies() const {
    std::vector<LatencySample> all;
    for (const auto& l : latency_) all.insert(all.end(), l.begin(), l.end());
    return all;
  }

private:
  std::vector<PaddedCounter> committed_;
  std::vector<std::vector<LatencySample>> latency_;  // one writer per vector
};

}  // namespace tuskflow

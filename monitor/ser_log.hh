#pragma once

// Serializability log (not in the original):
// --ser_log=<path>.  Checked by tools/ser_check.cc.

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace tuskflow {

// One record per committed transaction (the mammoth: one per epoch), as int32
// words: tid, kind, nreads, nwrites, (node, version) * nreads,
// (node, version) * nwrites.
enum SerKind : int32_t { kSerEpoch = 0, kSerTwoPL = 1, kSerMammoth = 2 };
using SerPairs = std::vector<std::pair<uint32_t, uint32_t>>;

class SerLog {
public:
  static bool on() { return enabled_; }
  static void enable() { enabled_ = true; }
  static void emit(int64_t tid, SerKind kind, const SerPairs& reads,
                   const SerPairs& writes) {
    std::vector<int32_t>& b = buffer();
    b.push_back(static_cast<int32_t>(tid));
    b.push_back(kind);
    b.push_back(static_cast<int32_t>(reads.size()));
    b.push_back(static_cast<int32_t>(writes.size()));
    for (const auto& r : reads) {
      b.push_back(static_cast<int32_t>(r.first));
      b.push_back(static_cast<int32_t>(r.second));
    }
    for (const auto& w : writes) {
      b.push_back(static_cast<int32_t>(w.first));
      b.push_back(static_cast<int32_t>(w.second));
    }
  }
  // Called after the workers have stopped.
  static size_t dump(const std::string& path) {
    FILE* f = std::fopen(path.c_str(), "wb");
    if (f == nullptr) return 0;
    size_t words = 0;
    std::lock_guard<std::mutex> g(mu());
    for (auto* b : buffers()) {
      std::fwrite(b->data(), sizeof(int32_t), b->size(), f);
      words += b->size();
    }
    std::fclose(f);
    return words;
  }

private:
  static inline bool enabled_ = false;
  static std::mutex& mu() {
    static std::mutex m;
    return m;
  }
  static std::vector<std::vector<int32_t>*>& buffers() {
    static std::vector<std::vector<int32_t>*> v;
    return v;
  }
  static std::vector<int32_t>& buffer() {
    thread_local std::vector<int32_t>* b = [] {
      auto* nb = new std::vector<int32_t>();
      std::lock_guard<std::mutex> g(mu());
      buffers().push_back(nb);
      return nb;
    }();
    return *b;
  }
};

}  // namespace tuskflow

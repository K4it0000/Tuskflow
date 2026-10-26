#pragma once

// Quiescent-state-based reclamation: stands in for the garbage collector for
// objects that other threads may still be reading after they were unlinked
// (hash-map nodes and tables, lock objects, lock clients).
//
// A thread registers lazily on its first retire() or quiescent().  Workers call
// quiescent() between tasks, a point where they hold no pointer into shared
// structures.  An object retired at era e is freed once every registered,
// online thread has announced an era > e.  Threads that never touch shared
// structures need not register; threads that register and then block for a
// long time call offline().

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <vector>

namespace tuskflow {

class Qsbr {
public:
  static Qsbr& instance() {
    static Qsbr q;
    return q;
  }

  // Called by a thread at a point where it holds no shared pointer.
  void quiescent() {
    ThreadState& ts = state();
    slots_[ts.slot].era.store(global_.load(std::memory_order_acquire),
                              std::memory_order_release);
    if (ts.retired.size() >= kReclaimBatch) reclaim(ts);
  }
  // Marks the calling thread as not holding shared pointers until its next
  // quiescent() call.
  void offline() {
    slots_[state().slot].era.store(0, std::memory_order_release);
  }

  template <typename T>
  void retire(T* p) {
    if (p == nullptr) return;
    ThreadState& ts = state();
    uint64_t era = global_.fetch_add(1, std::memory_order_acq_rel);
    ts.retired.push_back(
        {era, p, [](void* q) { delete static_cast<T*>(q); }, false});
  }
  // For objects that stay reachable through a stale field after they are
  // retired (a lock client's `waiting_for_lock_` may still point to a lock or
  // to the client owning it until that client clears its wait state, which
  // it does before its next quiescent point).  After one grace period nobody
  // can newly reach the object; a second one covers threads that reached it
  // in between.
  template <typename T>
  void retire_twice(T* p) {
    if (p == nullptr) return;
    ThreadState& ts = state();
    uint64_t era = global_.fetch_add(1, std::memory_order_acq_rel);
    ts.retired.push_back(
        {era, p, [](void* q) { delete static_cast<T*>(q); }, true});
  }

  uint64_t pending() {  // retired, not yet freed (approximate)
    uint64_t n = 0;
    for (int i = 0; i < kMaxThreads; ++i)
      n += slots_[i].pending.load(std::memory_order_relaxed);
    return n;
  }

private:
  static constexpr int kMaxThreads = 1024;
  static constexpr size_t kReclaimBatch = 256;

  struct Retired {
    uint64_t era;
    void* p;
    void (*del)(void*);
    bool second_grace;  // still needs a second grace period
  };
  struct alignas(64) Slot {
    std::atomic<uint64_t> era{0};  // 0 = offline / unused
    std::atomic<uint64_t> pending{0};
  };
  struct ThreadState {
    int slot = -1;
    std::vector<Retired> retired;
    ~ThreadState() {
      if (slot >= 0) Qsbr::instance().slots_[slot].era.store(0);
      // Leftovers are leaked: they may still be referenced and the process
      // is about to end (threads only exit at shutdown).
    }
  };

  std::atomic<uint64_t> global_{1};
  std::atomic<int> next_slot_{0};
  Slot slots_[kMaxThreads];

  ThreadState& state() {
    thread_local ThreadState ts;
    if (ts.slot < 0) {
      ts.slot = next_slot_.fetch_add(1);
      if (ts.slot >= kMaxThreads) std::abort();
      slots_[ts.slot].era.store(global_.load(), std::memory_order_release);
    }
    return ts;
  }

  void reclaim(ThreadState& ts) {
    uint64_t min_era = UINT64_MAX;
    int n = next_slot_.load(std::memory_order_acquire);
    for (int i = 0; i < n && i < kMaxThreads; ++i) {
      uint64_t e = slots_[i].era.load(std::memory_order_acquire);
      if (e != 0 && e < min_era) min_era = e;
    }
    size_t kept = 0;
    for (Retired& r : ts.retired) {
      if (r.era >= min_era) {
        ts.retired[kept++] = r;
      } else if (r.second_grace) {
        r.era = global_.fetch_add(1, std::memory_order_acq_rel);
        r.second_grace = false;
        ts.retired[kept++] = r;
      } else {
        r.del(r.p);
      }
    }
    ts.retired.resize(kept);
    slots_[ts.slot].pending.store(kept, std::memory_order_relaxed);
  }
};

}  // namespace tuskflow

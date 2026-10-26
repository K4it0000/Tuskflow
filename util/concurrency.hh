#pragma once

// Small runtime pieces the original takes from the JDK:
//   ConcurrentLinkedQueue -> ConcurrentQueue
//   ResettableCountDownLatch (AQS based) -> CountDownLatch
//   LockSupport.parkNanos(1) -> park_nanos_1()
//   java.util.Random -> JavaRandom (bit-exact)

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>

#include <time.h>

#include "util/chm.hh"

namespace tuskflow {

// LockSupport.parkNanos(1L): on Linux the sleep is rounded up to the timer
// slack (about 50 us by default), which nanosleep(1 ns) reproduces.
inline void park_nanos_1() {
  struct timespec ts{0, 1};
  nanosleep(&ts, nullptr);
}

inline double now_millis() {  // System.nanoTime() / 1_000_000 (as double)
  return static_cast<double>(
             std::chrono::duration_cast<std::chrono::nanoseconds>(
                 std::chrono::steady_clock::now().time_since_epoch())
                 .count()) /
         1e6;
}

// Unbounded MPMC FIFO queue.  Deviation: the original uses the lock-free
// ConcurrentLinkedQueue; this one takes a spin lock per operation.
template <typename T>
class ConcurrentQueue {
public:
  void add(T* v) {
    lock_.lock();
    q_.push_back(v);
    size_.fetch_add(1, std::memory_order_release);
    lock_.unlock();
  }
  T* poll() {
    if (size_.load(std::memory_order_acquire) == 0) return nullptr;
    lock_.lock();
    T* v = nullptr;
    if (!q_.empty()) {
      v = q_.front();
      q_.pop_front();
      size_.fetch_sub(1, std::memory_order_release);
    }
    lock_.unlock();
    return v;
  }
  bool empty() const { return size_.load(std::memory_order_acquire) == 0; }
  size_t size() const { return size_.load(std::memory_order_acquire); }
  template <typename F>
  void for_each(F&& f) {
    lock_.lock();
    for (T* v : q_)
      if (!f(v)) break;
    lock_.unlock();
  }

private:
  SpinLock lock_;
  std::deque<T*> q_;
  std::atomic<size_t> size_{0};
};

// ResettableCountDownLatch: reset(count), countDown(), await(timeout).
class CountDownLatch {
public:
  void reset(int count) {
    std::lock_guard<std::mutex> g(mu_);
    count_.store(count, std::memory_order_release);
  }
  void count_down() {
    if (count_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      std::lock_guard<std::mutex> g(mu_);
      cv_.notify_all();
    }
  }
  // Returns false on timeout.
  bool await_for(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(mu_);
    return cv_.wait_for(lk, timeout, [this] {
      return count_.load(std::memory_order_acquire) <= 0;
    });
  }
  int count() const { return count_.load(std::memory_order_acquire); }

private:
  std::atomic<int> count_{0};
  std::mutex mu_;
  std::condition_variable cv_;
};

// java.util.Random (48-bit LCG), bit-exact.
class JavaRandom {
public:
  explicit JavaRandom(int64_t seed)
      : seed_((static_cast<uint64_t>(seed) ^ kMultiplier) & kMask) {}

  int32_t next_int(int32_t bound) {  // Random.nextInt(bound)
    int32_t r = next(31);
    int32_t m = bound - 1;
    if ((bound & m) == 0) {
      r = static_cast<int32_t>((static_cast<int64_t>(bound) * r) >> 31);
    } else {
      // Java relies on int wrap-around here; do the same in unsigned.
      for (int32_t u = r;; u = next(31)) {
        r = u % bound;
        uint32_t t = static_cast<uint32_t>(u) - static_cast<uint32_t>(r) +
                     static_cast<uint32_t>(m);
        if (static_cast<int32_t>(t) >= 0) break;
      }
    }
    return r;
  }

private:
  static constexpr uint64_t kMultiplier = 0x5DEECE66DULL;
  static constexpr uint64_t kAddend = 0xBULL;
  static constexpr uint64_t kMask = (1ULL << 48) - 1;
  uint64_t seed_;

  int32_t next(int bits) {
    seed_ = (seed_ * kMultiplier + kAddend) & kMask;
    return static_cast<int32_t>(static_cast<int64_t>(seed_) >> (48 - bits));
  }
};

}  // namespace tuskflow

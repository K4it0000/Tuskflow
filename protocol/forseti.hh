#pragma once

// Neo4j's Forseti lock manager (neo4j-lock 5.16.0, the version the original
// links: org.neo4j.kernel.impl.locking.forseti.{ForsetiLockManager,
// ForsetiClient, SharedLock, ExclusiveLock, LockPath}), ported from the
// bytecode of that jar.  Only the paths TwoPLImpl uses are ported:
// newClient / initialize / acquireShared / acquireExclusive / close, with
// wait, deadlock detection and shared-to-exclusive upgrade.
//
// Omitted (no effect for this workload): LockTracer (NONE in the original),
// the memory tracker, lock acquisition timeout (0 = disabled by default),
// prepareForCommit (never called, so the "committing" checks are always
// false), the formatting of deadlock messages.

#include <sched.h>
#include <time.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <unordered_set>
#include <vector>

#include <emmintrin.h>

#include "util/chm.hh"
#include "util/qsbr.hh"

namespace tuskflow {

class DeadlockDetectedException : public std::runtime_error {
public:
  DeadlockDetectedException() : std::runtime_error("deadlock detected") {}
};

class ForsetiClient;
class Lock;

// Long.hashCode(clientId): ForsetiClient.hashCode / equals use clientId.
struct ClientHash {
  int32_t operator()(ForsetiClient* c) const;
};
using LockMap = ConcurrentHashMap<int64_t, Lock*>;
using ClientSet = ConcurrentHashMap<ForsetiClient*, bool, ClientHash>;
using ClientIdSet = ConcurrentHashMap<int64_t, bool>;  // keyed by clientId

// HeapTrackingLongIntHashMap (Eclipse LongIntHashMap): open addressing.
class LongIntMap {
public:
  LongIntMap() { alloc(16); }
  ~LongIntMap() {
    delete[] keys_;
    delete[] vals_;
    delete[] state_;
  }
  LongIntMap(const LongIntMap&) = delete;
  LongIntMap& operator=(const LongIntMap&) = delete;

  int get_if_absent(int64_t k, int absent) const {
    size_t i = find(k);
    return (state_[i] == kFull) ? vals_[i] : absent;
  }
  bool contains(int64_t k) const { return state_[find(k)] == kFull; }
  void put(int64_t k, int v) {
    size_t i = find(k);
    if (state_[i] == kFull) {
      vals_[i] = v;
      return;
    }
    if ((used_ + 1) * 2 > cap_) {
      rehash(cap_ * 2);
      i = find(k);
    }
    if (state_[i] == kEmpty) ++used_;
    keys_[i] = k;
    vals_[i] = v;
    state_[i] = kFull;
    ++size_;
  }
  void remove(int64_t k) {
    size_t i = find(k);
    if (state_[i] == kFull) {
      state_[i] = kRemoved;
      --size_;
    }
  }
  int remove_key_if_absent(int64_t k, int absent) {
    size_t i = find(k);
    if (state_[i] != kFull) return absent;
    state_[i] = kRemoved;
    --size_;
    return vals_[i];
  }
  template <typename F>
  void for_each_key(F&& f) const {
    for (size_t i = 0; i < cap_; ++i)
      if (state_[i] == kFull) f(keys_[i]);
  }

private:
  static constexpr uint8_t kEmpty = 0, kFull = 1, kRemoved = 2;
  int64_t* keys_ = nullptr;
  int* vals_ = nullptr;
  uint8_t* state_ = nullptr;
  size_t cap_ = 0, used_ = 0, size_ = 0;

  void alloc(size_t cap) {
    cap_ = cap;
    keys_ = new int64_t[cap];
    vals_ = new int[cap];
    state_ = new uint8_t[cap]();
    used_ = size_ = 0;
  }
  static uint64_t mix(int64_t k) {
    uint64_t x = static_cast<uint64_t>(k) * 0x9e3779b97f4a7c15ULL;
    return x ^ (x >> 29);
  }
  // First slot holding k, or the first reusable slot on its probe path.
  size_t find(int64_t k) const {
    size_t mask = cap_ - 1, i = mix(k) & mask, reuse = SIZE_MAX;
    for (;;) {
      if (state_[i] == kEmpty) return reuse != SIZE_MAX ? reuse : i;
      if (state_[i] == kFull && keys_[i] == k) return i;
      if (state_[i] == kRemoved && reuse == SIZE_MAX) reuse = i;
      i = (i + 1) & mask;
    }
  }
  void rehash(size_t cap) {
    int64_t* ok = keys_;
    int* ov = vals_;
    uint8_t* os = state_;
    size_t oc = cap_;
    alloc(cap);
    for (size_t i = 0; i < oc; ++i)
      if (os[i] == kFull) put(ok[i], ov[i]);
    delete[] ok;
    delete[] ov;
    delete[] os;
  }
};

enum class LockType : uint8_t { SHARED, EXCLUSIVE };

class Lock {
public:
  virtual ~Lock() = default;
  virtual void copy_holder_wait_lists_into(ClientIdSet& set) = 0;
  virtual ForsetiClient* detect_deadlock(ForsetiClient* client) = 0;
  virtual void collect_owners(std::unordered_set<ForsetiClient*>& set) = 0;
  virtual bool is_closed() = 0;
  virtual bool is_shared() const = 0;
};

class SharedLock final : public Lock {
public:
  static constexpr int32_t kUpdateLockFlag = INT32_MIN;
  explicit SharedLock(ForsetiClient* client) { holders_.add(client); }

  bool acquire(ForsetiClient* client) {
    if (!acquire_reference()) return false;
    if (holders_.add(client)) return true;
    release_reference();
    return false;
  }
  bool release(ForsetiClient* client) {
    if (!holders_.remove(client)) std::abort();  // IllegalStateException
    return release_reference();
  }
  void copy_holder_wait_lists_into(ClientIdSet& set) override;
  ForsetiClient* detect_deadlock(ForsetiClient* client) override;
  void collect_owners(std::unordered_set<ForsetiClient*>& set) override {
    holders_.for_each([&](ForsetiClient* c, bool) { set.insert(c); });
  }
  bool is_closed() override { return number_of_holders() == 0; }
  bool is_shared() const override { return true; }

  bool try_acquire_update_lock() {
    return (ref_count_.fetch_or(kUpdateLockFlag) & kUpdateLockFlag) == 0;
  }
  void release_update_lock() { ref_count_.fetch_and(INT32_MAX); }
  int number_of_holders() const { return ref_count_.load() & INT32_MAX; }
  bool is_update_lock() const {
    return (ref_count_.load() & kUpdateLockFlag) == kUpdateLockFlag;
  }

private:
  std::atomic<int32_t> ref_count_{1};
  ClientSet holders_;  // ConcurrentHashMap.newKeySet()

  bool acquire_reference() {
    for (;;) {
      int32_t r = ref_count_.load();
      if (r <= 0) return false;  // closed, or update lock taken
      if (ref_count_.compare_exchange_weak(r, r + 1)) return true;
    }
  }
  bool release_reference() {  // true when the last holder left
    for (;;) {
      int32_t r = ref_count_.load();
      int32_t n = (r & INT32_MAX) - 1;
      if (ref_count_.compare_exchange_weak(r, n | (r & kUpdateLockFlag)))
        return n == 0;
    }
  }
};

class ExclusiveLock final : public Lock {
public:
  explicit ExclusiveLock(ForsetiClient* owner) : owner_(owner) {}
  void copy_holder_wait_lists_into(ClientIdSet& set) override;
  ForsetiClient* detect_deadlock(ForsetiClient* client) override;
  void collect_owners(std::unordered_set<ForsetiClient*>& set) override {
    set.insert(owner_);
  }
  bool is_closed() override { return closed_.load(); }
  bool is_shared() const override { return false; }
  void close() { closed_.store(true); }

private:
  ForsetiClient* owner_;
  std::atomic<bool> closed_{false};
};

inline void park_nanos(long ns) {
  struct timespec ts{0, ns};
  nanosleep(&ts, nullptr);
}

struct LockStats {  // not in the original
  std::atomic<uint64_t> waits{0};       // waitFor() calls
  std::atomic<uint64_t> deadlocks{0};   // DeadlockDetectedException thrown
  std::atomic<uint64_t> upgrades{0};    // shared -> exclusive
  // Debug: outcome of the deadlock checks in long waits (tries > 100).
  std::atomic<uint64_t> long_no_other{0};     // detectDeadlock found nobody
  std::atomic<uint64_t> long_not_abort{0};    // found one, but we don't abort
  std::atomic<uint64_t> long_not_real{0};     // isDeadlockReal == -1
  std::atomic<uint64_t> long_no_path{0};      // findDeadlockPath == null
};
inline LockStats g_lock_stats;

class ForsetiClient {
public:
  static constexpr int kTypes = 3;

  ForsetiClient(LockMap* lock_maps, int64_t client_id)
      : lock_maps_(lock_maps), client_id_(client_id) {}
  ~ForsetiClient() {
    for (int i = 0; i < kTypes; ++i) {
      delete shared_counts_[i];
      delete exclusive_counts_[i];
    }
    delete my_exclusive_lock_;
  }
  ForsetiClient(const ForsetiClient&) = delete;
  ForsetiClient& operator=(const ForsetiClient&) = delete;

  void initialize(int64_t transaction_id) {
    state_.store(0);
    transaction_id_.store(transaction_id);
    my_exclusive_lock_ = new ExclusiveLock(this);
  }

  void acquire_shared(int type, int64_t id) {
    has_locks_ = true;
    increment_active_clients();
    bool waited = false;
    try {
      LockMap& map = lock_maps_[type];
      LongIntMap& shared = shared_count(type);
      LongIntMap& excl = exclusive_count(type);
      int held = shared.get_if_absent(id, -1);
      if (held != -1) {
        shared.put(id, held + 1);
      } else if (excl.contains(id)) {
        shared.put(id, 1);
      } else {
        int tries = 0;
        SharedLock* my_shared = nullptr;
        bool published = false;
        for (;;) {
          assert_valid();
          Lock* existing = nullptr;
          if (!map.get(id, existing)) existing = nullptr;
          if (existing == nullptr) {
            if (my_shared == nullptr) my_shared = new SharedLock(this);
            if (map.put_if_absent(id, my_shared) == nullptr) {
              published = true;
              break;
            }
            continue;
          }
          if (existing->is_shared() &&
              static_cast<SharedLock*>(existing)->acquire(this))
            break;
          waited = true;
          try {
            wait_for(existing, type, id, LockType::SHARED, tries++);
          } catch (...) {
            delete my_shared;  // never published
            throw;
          }
        }
        if (my_shared != nullptr && !published) delete my_shared;  // garbage
        active_lock_count_.fetch_add(1);
        shared.put(id, 1);
      }
    } catch (...) {
      if (waited) clear_wait_state();
      decrement_active_clients();
      throw;
    }
    if (waited) clear_wait_state();
    decrement_active_clients();
  }

  void acquire_exclusive(int type, int64_t id) {
    has_locks_ = true;
    increment_active_clients();
    bool waited = false;
    try {
      LockMap& map = lock_maps_[type];
      LongIntMap& excl = exclusive_count(type);
      int held = excl.get_if_absent(id, -1);
      if (held != -1) {
        excl.put(id, held + 1);
      } else {
        int tries = 0;
        bool upgraded = false;
        for (;;) {
          Lock* existing = map.put_if_absent(id, my_exclusive_lock_);
          if (existing == nullptr) break;
          assert_valid();
          if (existing->is_shared() &&
              try_upgrade_shared_to_exclusive(
                  type, map, id, static_cast<SharedLock*>(existing))) {
            upgraded = true;
            break;
          }
          waited = true;
          wait_for(existing, type, id, LockType::EXCLUSIVE, tries++);
        }
        if (!upgraded) active_lock_count_.fetch_add(1);
        excl.put(id, 1);
      }
    } catch (...) {
      if (waited) clear_wait_state();
      decrement_active_clients();
      throw;
    }
    if (waited) clear_wait_state();
    decrement_active_clients();
  }

  // close(): releases every lock.  The caller retires the client afterwards.
  void close() {
    for (;;) {  // LockClientStateHolder.closeClient: set STOPPED
      int32_t s = state_.load();
      if (state_.compare_exchange_weak(s, (s & kClientMask) | kStopped)) break;
    }
    while ((state_.load() & kClientMask) > 0) park_nanos(10'000'000);
    if (has_locks_) {
      release_all_client_locks();
      wait_list_.clear();
      has_locks_ = false;
    }
    transaction_id_.store(-1);
    my_exclusive_lock_->close();
  }

  // Read by other clients during deadlock detection.
  int64_t transaction_id() const { return transaction_id_.load(); }
  int64_t client_id() const { return client_id_; }
  int64_t active_lock_count() const { return active_lock_count_.load(); }
  Lock* waiting_for_lock() const { return waiting_for_lock_.load(); }
  int waiting_for_type() const { return waiting_for_type_.load(); }

  void copy_wait_list_to(ClientIdSet& set) {
    set.add(client_id_);
    wait_list_.for_each([&](int64_t c, bool) { set.add(c); });
  }
  bool is_waiting_for(ForsetiClient* client) {
    return client->transaction_id() != transaction_id() &&
           wait_list_.contains(client->client_id_);
  }

private:
  static constexpr int32_t kStopped = 1 << 30;
  static constexpr int32_t kClientMask = (1 << 29) - 1;

  LockMap* lock_maps_;
  LongIntMap* shared_counts_[kTypes] = {};
  LongIntMap* exclusive_counts_[kTypes] = {};
  std::atomic<int64_t> active_lock_count_{0};
  ClientIdSet wait_list_;
  std::atomic<int32_t> state_{0};
  ExclusiveLock* my_exclusive_lock_ = nullptr;
  bool has_locks_ = false;
  std::atomic<Lock*> waiting_for_lock_{nullptr};
  std::atomic<int> waiting_for_type_{-1};
  std::atomic<int64_t> waiting_for_id_{-1};
  std::atomic<int64_t> transaction_id_{-1};
  int64_t client_id_;

  LongIntMap& shared_count(int type) {
    if (shared_counts_[type] == nullptr) shared_counts_[type] = new LongIntMap();
    return *shared_counts_[type];
  }
  LongIntMap& exclusive_count(int type) {
    if (exclusive_counts_[type] == nullptr)
      exclusive_counts_[type] = new LongIntMap();
    return *exclusive_counts_[type];
  }

  void increment_active_clients() {
    for (;;) {
      int32_t s = state_.load();
      if (s & kStopped) std::abort();  // LockClientStoppedException
      if (state_.compare_exchange_weak(s, s + 1)) return;
    }
  }
  void decrement_active_clients() {
    for (;;) {
      int32_t s = state_.load();
      if (state_.compare_exchange_weak(s, s - 1)) return;
    }
  }
  void assert_valid() {  // assertNotStopped; the timeout is disabled
    if (state_.load() & kStopped) std::abort();
  }

  void clear_wait_state() {
    wait_list_.clear();
    waiting_for_lock_.store(nullptr);
    waiting_for_id_.store(-1);
    waiting_for_type_.store(-1);
  }

  bool try_upgrade_shared_to_exclusive(int type, LockMap& map, int64_t id,
                                       SharedLock* lock) {
    int tries = 0;
    if (!shared_count(type).contains(id)) {
      if (!lock->acquire(this)) return false;
      active_lock_count_.fetch_add(1);
      try {
        if (try_upgrade_with_share_held(type, id, lock, tries)) return true;
        release_global_lock(map, id);
        return false;
      } catch (...) {
        release_global_lock(map, id);
        throw;
      }
    }
    return try_upgrade_with_share_held(type, id, lock, tries);
  }

  bool try_upgrade_with_share_held(int type, int64_t id, SharedLock* lock,
                                   int tries) {
    if (!lock->try_acquire_update_lock()) return false;
    bool waited = false;
    try {
      while (lock->number_of_holders() > 1) {
        assert_valid();
        waited = true;
        wait_for(lock, type, id, LockType::EXCLUSIVE, tries++);
      }
    } catch (...) {
      lock->release_update_lock();
      if (waited) clear_wait_state();
      throw;
    }
    if (waited) clear_wait_state();
    g_lock_stats.upgrades.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  void release_global_lock(LockMap& map, int64_t id) {
    Lock* lock = nullptr;
    if (map.get(id, lock) && lock != nullptr) {
      if (!lock->is_shared()) {
        map.remove(id);  // our own ExclusiveLock, freed with this client
      } else if (static_cast<SharedLock*>(lock)->release(this)) {
        map.remove(id);
        Qsbr::instance().retire_twice(static_cast<SharedLock*>(lock));
      }
    }
    active_lock_count_.fetch_sub(1);
  }

  void release_all_client_locks() {
    for (int i = 0; i < kTypes; ++i) {
      LongIntMap* excl = exclusive_counts_[i];
      LongIntMap* shared = shared_counts_[i];
      if (excl != nullptr) {
        excl->for_each_key([&](int64_t k) {
          release_global_lock(lock_maps_[i], k);
          if (shared != nullptr) shared->remove(k);
        });
        exclusive_counts_[i] = nullptr;
        delete excl;
      }
      if (shared != nullptr) {
        shared->for_each_key(
            [&](int64_t k) { release_global_lock(lock_maps_[i], k); });
        shared_counts_[i] = nullptr;
        delete shared;
      }
    }
    active_lock_count_.store(0);
  }

  static void incremental_backoff_wait(long tries) {
    if (tries < 1000) {
      _mm_pause();  // Thread.onSpinWait
    } else if (tries < 1002) {
      park_nanos(500);  // LockSupport.parkNanos(500)
    } else {
      park_nanos(1'000'000);  // Thread.sleep(1)
    }
  }

  void wait_for(Lock* lock, int type, int64_t id, LockType, int tries) {
    g_lock_stats.waits.fetch_add(1, std::memory_order_relaxed);
    // clearAndCopyWaitList
    wait_list_.clear();
    lock->copy_holder_wait_lists_into(wait_list_);
    // noteWaitingForLock
    waiting_for_type_.store(type);
    waiting_for_id_.store(id);
    waiting_for_lock_.store(lock);
    incremental_backoff_wait(tries);
    ForsetiClient* other = lock->detect_deadlock(this);
    if (tries > 100 && other == nullptr)
      g_lock_stats.long_no_other.fetch_add(1, std::memory_order_relaxed);
    if (other != nullptr && should_abort(other)) {
      if (tries > 100) {
        int depth = is_deadlock_real(lock);
        // lock_manager_verbose_deadlocks = true in the original's Config:
        // the exception is thrown only if a wait cycle path is found.
        if (depth == -1)
          g_lock_stats.long_not_real.fetch_add(1, std::memory_order_relaxed);
        if (depth != -1 && find_deadlock_path(lock, depth)) {
          g_lock_stats.deadlocks.fetch_add(1, std::memory_order_relaxed);
          throw DeadlockDetectedException();
        }
        if (depth != -1)
          g_lock_stats.long_no_path.fetch_add(1, std::memory_order_relaxed);
      }
      sched_yield();  // Thread.yield
    } else if (other != nullptr && tries > 100) {
      g_lock_stats.long_not_abort.fetch_add(1, std::memory_order_relaxed);
    }
    // (tries & 8191) == 8191: checks for a client committing on this thread;
    // prepareForCommit is never called, so it never fires.
  }

  bool should_abort(ForsetiClient* other) {
    if (transaction_id() == other->transaction_id()) return true;
    int64_t mine = active_lock_count(), theirs = other->active_lock_count();
    if (mine > theirs) return false;
    if (theirs > mine) return true;
    return transaction_id() > other->transaction_id();
  }

  int is_deadlock_real(Lock* lock) {
    if (is_deadlock_real_internal(lock) != -1) {
      park_nanos(10'000'000);  // 10 ms, then check again
      return is_deadlock_real_internal(lock);
    }
    return -1;
  }

  int is_deadlock_real_internal(Lock* lock) {
    std::unordered_set<Lock*> seen_locks, next_locks;
    std::unordered_set<ForsetiClient*> owners, next_owners;
    lock->collect_owners(owners);
    int depth = 1;
    do {
      ++depth;
      seen_locks.insert(next_locks.begin(), next_locks.end());
      // collectNextOwners (never "committing", see wait_for)
      next_locks.clear();
      for (ForsetiClient* o : owners) {
        Lock* w = o->waiting_for_lock();
        if (w != nullptr && !w->is_closed() && !seen_locks.count(w))
          next_locks.insert(w);
      }
      for (Lock* l : next_locks) l->collect_owners(next_owners);
      if (next_owners.count(this) && lock->detect_deadlock(this) != nullptr)
        return depth;
      owners.clear();
      std::swap(owners, next_owners);
    } while (!next_locks.empty());
    return -1;
  }

  // findDeadlockPath: whether a wait chain from `lock` leads back to this
  // client within depth + 1 steps (the message itself is not built).
  struct LockPath {
    int64_t owner_tid;
    Lock* owner_waiting_for;
    const LockPath* parent;
    bool contains_owner(int64_t tid) const {
      for (const LockPath* p = this; p != nullptr; p = p->parent)
        if (p->owner_tid == tid) return true;
      return false;
    }
  };
  bool find_deadlock_path(Lock* lock, int depth) {
    std::vector<std::unique_ptr<LockPath>> pool;
    std::vector<const LockPath*> paths, next_paths;
    std::unordered_set<ForsetiClient*> owners;
    traverse_one_step(nullptr, lock, paths, 0, owners, pool);
    for (int i = 1; i <= depth + 1; ++i) {
      for (const LockPath* p : paths) {
        Lock* w = p->owner_waiting_for;
        if (!w->is_closed() &&
            traverse_one_step(p, w, next_paths, i, owners, pool) != nullptr)
          return true;
      }
      paths.swap(next_paths);
      next_paths.clear();
    }
    return false;
  }
  const LockPath* traverse_one_step(
      const LockPath* parent, Lock* lock, std::vector<const LockPath*>& out,
      int depth, std::unordered_set<ForsetiClient*>& owners,
      std::vector<std::unique_ptr<LockPath>>& pool) {
    owners.clear();
    lock->collect_owners(owners);
    for (ForsetiClient* c : owners) {
      Lock* w = c->waiting_for_lock();
      int wtype = c->waiting_for_type();
      int64_t tid = c->transaction_id();
      if (w == nullptr || wtype < 0 || w->is_closed()) continue;
      if (parent != nullptr && parent->contains_owner(tid)) continue;
      pool.push_back(std::make_unique<LockPath>(LockPath{tid, w, parent}));
      const LockPath* path = pool.back().get();
      if (c == this && depth > 0) return path;
      out.push_back(path);
    }
    return nullptr;
  }
};

inline int32_t ClientHash::operator()(ForsetiClient* c) const {
  return java_hash(c->client_id());
}

inline void SharedLock::copy_holder_wait_lists_into(ClientIdSet& set) {
  holders_.for_each([&](ForsetiClient* c, bool) { c->copy_wait_list_to(set); });
}
inline ForsetiClient* SharedLock::detect_deadlock(ForsetiClient* client) {
  if (is_closed()) return nullptr;
  ForsetiClient* found = nullptr;
  holders_.for_each([&](ForsetiClient* c, bool) {
    if (found == nullptr && c->is_waiting_for(client)) found = c;
  });
  return found;
}
inline void ExclusiveLock::copy_holder_wait_lists_into(ClientIdSet& set) {
  owner_->copy_wait_list_to(set);
}
inline ForsetiClient* ExclusiveLock::detect_deadlock(ForsetiClient* client) {
  return (!closed_.load() && owner_->is_waiting_for(client)) ? owner_ : nullptr;
}

class ForsetiLockManager {
public:
  // One `new ConcurrentHashMap<>(16, 0.6f, 512)` per resource type.
  ForsetiLockManager()
      : lock_maps_{LockMap(16, 0.6, 512), LockMap(16, 0.6, 512),
                   LockMap(16, 0.6, 512)} {}
  ForsetiClient* new_client() {
    return new ForsetiClient(lock_maps_, client_ids_.fetch_add(1) + 1);
  }
  int64_t locks(int type) const { return lock_maps_[type].size(); }

private:
  LockMap lock_maps_[ForsetiClient::kTypes];
  std::atomic<int64_t> client_ids_{0};
};

}  // namespace tuskflow

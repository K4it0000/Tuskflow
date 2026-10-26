#pragma once

// Concurrent hash map modelled on java.util.concurrent.ConcurrentHashMap
// (JDK 8+), used where the original uses one: the reservation table, the
// access-metadata store, and Neo4j's Forseti lock manager (lock maps, the
// holder set of a shared lock, the wait list of a lock client).
//
// Kept from the JDK design:
//   - lazily allocated table of bins, each bin a linked chain, power-of-two
//     size; the sizing rules of the constructors;
//   - Java's hash (Long/Integer.hashCode, or the key's own hashCode), then
//     spread(): (h ^ (h >>> 16)) & 0x7fffffff, bin = hash & (n - 1);
//   - insertion into an empty bin by CAS, otherwise under a per-bin lock,
//     appending at the tail; lock-free reads;
//   - the element count as a base counter plus counter cells that are
//     created on contention, and the resize check of addCount();
//   - resize to 2n when the count reaches 0.75 n: bins are copied into the
//     new table and the old bin is replaced by a forwarding marker, so reads
//     and writes continue during a resize.
// Deviations:
//   - long bins are not converted to trees;
//   - one thread transfers all bins of a resize (the JDK lets writers help);
//     writers that find a forwarding marker continue in the new table;
//   - unlinked nodes and replaced tables are freed through Qsbr instead of
//     the garbage collector.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include <emmintrin.h>

#include "util/qsbr.hh"

namespace tuskflow {

inline int32_t java_hash(int64_t key) {  // Long.hashCode
  uint64_t v = static_cast<uint64_t>(key);
  return static_cast<int32_t>(static_cast<uint32_t>(v ^ (v >> 32)));
}
inline int32_t java_hash(int32_t key) { return key; }  // Integer.hashCode

template <typename K>
struct JavaHash {
  int32_t operator()(const K& k) const { return java_hash(k); }
};

inline int32_t spread(int32_t h) {
  uint32_t u = static_cast<uint32_t>(h);
  return static_cast<int32_t>((u ^ (u >> 16)) & 0x7fffffffu);
}

inline size_t table_size_for(size_t c) {  // smallest power of two >= c
  size_t n = 1;
  while (n < c) n <<= 1;
  return n;
}

class SpinLock {
public:
  void lock() {
    while (flag_.exchange(true, std::memory_order_acquire))
      while (flag_.load(std::memory_order_relaxed)) _mm_pause();
  }
  void unlock() { flag_.store(false, std::memory_order_release); }

private:
  std::atomic<bool> flag_{false};
};

template <typename K, typename V, typename Hash = JavaHash<K>>
class ConcurrentHashMap {
  static_assert(std::is_trivially_copyable_v<V>, "values are stored atomically");

  struct Node {
    K key;
    int32_t hash;
    std::atomic<V> val;
    std::atomic<Node*> next;
    Node(K k, int32_t h, V v) : key(k), hash(h), val(v), next(nullptr) {}
  };
  struct Bin {
    std::atomic<Node*> head{nullptr};
    SpinLock lock;
  };
  struct Table {
    size_t n;
    Bin* bins;
    std::atomic<Table*> next{nullptr};  // set when this table is replaced
    explicit Table(size_t n_) : n(n_), bins(new Bin[n_]) {}
    ~Table() { delete[] bins; }
  };
  static Node* moved() { return reinterpret_cast<Node*>(uintptr_t{1}); }

  static constexpr int kCells = 64;
  struct alignas(64) Cell {
    std::atomic<int64_t> v{0};
  };

public:
  // `new ConcurrentHashMap<>()`: 16 bins on first insertion.
  ConcurrentHashMap() : initial_n_(16) {}
  // `new ConcurrentHashMap<>(initialCapacity, loadFactor, concurrencyLevel)`.
  ConcurrentHashMap(size_t initial_capacity, double load_factor,
                    size_t concurrency_level) {
    if (initial_capacity < concurrency_level)
      initial_capacity = concurrency_level;
    size_t size = static_cast<size_t>(
        1.0 + static_cast<double>(initial_capacity) / load_factor);
    initial_n_ = table_size_for(size);
  }

  ~ConcurrentHashMap() {  // only at shutdown, with no concurrent users
    Table* t = table_.load(std::memory_order_relaxed);
    if (t != nullptr) {
      clear_exclusive();
      delete t;
    }
    delete[] cells_.load(std::memory_order_relaxed);
  }

  ConcurrentHashMap(const ConcurrentHashMap&) = delete;
  ConcurrentHashMap& operator=(const ConcurrentHashMap&) = delete;

  // Map.merge(key, value, remapping): `make()` creates the value of an absent
  // key; for a present key `update(old)` returns the new value (it may also
  // modify what the old value points to).  Returns the resulting value.
  template <typename Make, typename Update>
  V merge(K key, Make&& make, Update&& update) {
    V result{};
    put_val(key, &make, &update, /*only_if_absent=*/false, /*replace=*/false,
            &result);
    return result;
  }

  // Map.putIfAbsent: returns the existing value, or V{} if `value` was put.
  V put_if_absent(K key, V value) {
    V result{};
    auto make = [&] { return value; };
    bool existed = put_val(key, &make, static_cast<V (*)(V)>(nullptr), true,
                           false, &result);
    return existed ? result : V{};
  }

  // Map.put: returns the previous value, or V{}.
  V put(K key, V value) {
    V result{};
    auto make = [&] { return value; };
    bool existed = put_val(key, &make, static_cast<V (*)(V)>(nullptr), false,
                           true, &result);
    return existed ? result : V{};
  }

  // Set.add (KeySetView): true if the key was not present.
  bool add(K key) {
    V result{};
    auto make = [] { return V{}; };
    return !put_val(key, &make, static_cast<V (*)(V)>(nullptr), true, false,
                    &result);
  }

  // Map.remove: returns true and the old value if the key was present.
  bool remove(K key, V* old = nullptr) {
    const int32_t h = spread(hash_(key));
    Table* tab = table_.load(std::memory_order_acquire);
    while (tab != nullptr) {
      Bin& b = tab->bins[static_cast<size_t>(h) & (tab->n - 1)];
      Node* f = b.head.load(std::memory_order_acquire);
      if (f == nullptr) return false;
      if (f == moved()) {
        tab = tab->next.load(std::memory_order_acquire);
        continue;
      }
      b.lock.lock();
      f = b.head.load(std::memory_order_acquire);
      if (f == moved()) {
        b.lock.unlock();
        continue;
      }
      Node* prev = nullptr;
      for (Node* e = f; e != nullptr;
           prev = e, e = e->next.load(std::memory_order_relaxed)) {
        if (e->hash == h && e->key == key) {
          Node* nx = e->next.load(std::memory_order_relaxed);
          if (prev == nullptr)
            b.head.store(nx, std::memory_order_release);
          else
            prev->next.store(nx, std::memory_order_release);
          b.lock.unlock();
          if (old != nullptr) *old = e->val.load(std::memory_order_acquire);
          Qsbr::instance().retire(e);
          add_count(-1, -1);
          return true;
        }
      }
      b.lock.unlock();
      return false;
    }
    return false;
  }

  // Map.get / getOrDefault.  Lock-free.
  bool get(K key, V& out) const {
    const int32_t h = spread(hash_(key));
    Table* tab = table_.load(std::memory_order_acquire);
    while (tab != nullptr) {
      Node* e = tab->bins[static_cast<size_t>(h) & (tab->n - 1)].head.load(
          std::memory_order_acquire);
      if (e == moved()) {
        tab = tab->next.load(std::memory_order_acquire);
        continue;
      }
      for (; e != nullptr; e = e->next.load(std::memory_order_acquire)) {
        if (e->hash == h && e->key == key) {
          out = e->val.load(std::memory_order_acquire);
          return true;
        }
      }
      return false;
    }
    return false;
  }
  bool contains(K key) const {
    V v;
    return get(key, v);
  }

  // Weakly consistent traversal, f(key, value).
  template <typename F>
  void for_each(F&& f) const {
    Table* tab = table_.load(std::memory_order_acquire);
    if (tab == nullptr) return;
    for (size_t i = 0; i < tab->n; ++i) visit_bin(tab, i, f);
  }

  // Map.clear(): bin by bin under the bin lock; safe with concurrent users.
  void clear() {
    Table* tab = table_.load(std::memory_order_acquire);
    while (tab != nullptr) {
      for (size_t i = 0; i < tab->n; ++i) {
        Bin& b = tab->bins[i];
        if (b.head.load(std::memory_order_acquire) == nullptr) continue;
        b.lock.lock();
        Node* e = b.head.load(std::memory_order_acquire);
        if (e == moved()) {  // its entries are cleared in the next table
          b.lock.unlock();
          continue;
        }
        b.head.store(nullptr, std::memory_order_release);
        b.lock.unlock();
        int64_t removed = 0;
        while (e != nullptr) {
          Node* nx = e->next.load(std::memory_order_relaxed);
          Qsbr::instance().retire(e);
          e = nx;
          ++removed;
        }
        if (removed != 0) add_count(-removed, -1);
      }
      tab = tab->next.load(std::memory_order_acquire);
    }
  }

  // clear() for a map no other thread is using: frees immediately.
  void clear_exclusive() {
    Table* tab = table_.load(std::memory_order_relaxed);
    if (tab == nullptr) return;
    for (size_t i = 0; i < tab->n; ++i) {
      Node* e = tab->bins[i].head.load(std::memory_order_relaxed);
      tab->bins[i].head.store(nullptr, std::memory_order_relaxed);
      while (e != nullptr) {
        Node* nx = e->next.load(std::memory_order_relaxed);
        delete e;
        e = nx;
      }
    }
    base_.store(0, std::memory_order_relaxed);
    Cell* cells = cells_.load(std::memory_order_relaxed);
    if (cells != nullptr)
      for (int i = 0; i < kCells; ++i) cells[i].v.store(0);
  }

  int64_t size() const {  // sumCount
    int64_t s = base_.load(std::memory_order_relaxed);
    Cell* cells = cells_.load(std::memory_order_acquire);
    if (cells != nullptr)
      for (int i = 0; i < kCells; ++i)
        s += cells[i].v.load(std::memory_order_relaxed);
    return s;
  }
  bool empty() const { return size() <= 0; }
  size_t bins() const {
    Table* t = table_.load(std::memory_order_acquire);
    return t == nullptr ? 0 : t->n;
  }
  uint64_t resizes() const { return resizes_.load(std::memory_order_relaxed); }
  uint64_t resize_ns() const {
    return resize_ns_.load(std::memory_order_relaxed);
  }
  // Bytes held by bins and nodes (pointed-to values are counted by callers).
  size_t approx_bytes() const {
    return bins() * sizeof(Bin) + static_cast<size_t>(size()) * sizeof(Node);
  }

private:
  Hash hash_{};
  std::atomic<Table*> table_{nullptr};
  size_t initial_n_;
  std::atomic<int64_t> size_ctl_{0};
  std::atomic<bool> resizing_{false};
  std::atomic<int64_t> base_{0};
  std::atomic<Cell*> cells_{nullptr};
  std::atomic<uint64_t> resizes_{0};
  std::atomic<uint64_t> resize_ns_{0};

  static int64_t threshold(size_t n) {
    return static_cast<int64_t>(n - (n >> 2));  // 0.75 n, as sizeCtl
  }

  Table* init_table() {  // initTable(): lazily, once
    Table* t = table_.load(std::memory_order_acquire);
    if (t != nullptr) return t;
    Table* nt = new Table(initial_n_);
    if (table_.compare_exchange_strong(t, nt, std::memory_order_acq_rel)) {
      size_ctl_.store(threshold(initial_n_), std::memory_order_release);
      return nt;
    }
    delete nt;
    return t;
  }

  // putVal / compute.  Returns true if the key existed; *result receives the
  // existing value (put_if_absent), the previous value (replace) or the
  // merged value (update).
  template <typename Make, typename Update>
  bool put_val(K key, Make* make, Update update, bool only_if_absent,
               bool replace, V* result) {
    const int32_t h = spread(hash_(key));
    Node* fresh = nullptr;
    Table* tab = init_table();
    for (;;) {
      Bin& b = tab->bins[static_cast<size_t>(h) & (tab->n - 1)];
      Node* f = b.head.load(std::memory_order_acquire);
      if (f == nullptr) {
        if (fresh == nullptr) fresh = new Node(key, h, (*make)());
        Node* expected = nullptr;
        if (b.head.compare_exchange_strong(expected, fresh,
                                           std::memory_order_acq_rel)) {
          *result = fresh->val.load(std::memory_order_relaxed);
          add_count(1, 0);
          return false;
        }
        continue;
      }
      if (f == moved()) {
        tab = tab->next.load(std::memory_order_acquire);
        continue;
      }
      if (only_if_absent && f->hash == h && f->key == key) {
        // JDK putVal: a matching first node is returned without locking.
        *result = f->val.load(std::memory_order_acquire);
        delete fresh;
        return true;
      }
      b.lock.lock();
      f = b.head.load(std::memory_order_acquire);
      if (f == moved() || f == nullptr) {  // moved or emptied meanwhile
        b.lock.unlock();
        continue;
      }
      int bin_count = 1;
      Node* e = f;
      for (;; ++bin_count) {
        if (e->hash == h && e->key == key) {
          V old = e->val.load(std::memory_order_acquire);
          if (only_if_absent) {
            *result = old;
          } else if (replace) {
            e->val.store((*make)(), std::memory_order_release);
            *result = old;
          } else {
            V nv = (*update)(old);
            e->val.store(nv, std::memory_order_release);
            *result = nv;
          }
          b.lock.unlock();
          delete fresh;  // never published
          return true;
        }
        Node* nx = e->next.load(std::memory_order_relaxed);
        if (nx == nullptr) break;
        e = nx;
      }
      if (fresh == nullptr) fresh = new Node(key, h, (*make)());
      e->next.store(fresh, std::memory_order_release);
      *result = fresh->val.load(std::memory_order_relaxed);
      b.lock.unlock();
      add_count(1, bin_count);
      return false;
    }
  }

  template <typename F>
  void visit_bin(Table* tab, size_t i, F& f) const {
    Node* e = tab->bins[i].head.load(std::memory_order_acquire);
    if (e == moved()) {
      Table* nt = tab->next.load(std::memory_order_acquire);
      visit_bin(nt, i, f);
      visit_bin(nt, i + tab->n, f);
      return;
    }
    for (; e != nullptr; e = e->next.load(std::memory_order_acquire))
      f(e->key, e->val.load(std::memory_order_acquire));
  }

  static size_t probe() {
    static std::atomic<size_t> next{0};
    thread_local size_t p = next.fetch_add(1, std::memory_order_relaxed);
    return p % kCells;
  }

  // addCount(x, check): CAS the base counter; on contention switch to the
  // cells.  check < 0: no resize check; check <= 1 with contention: skip it.
  void add_count(int64_t x, int check) {
    Cell* cells = cells_.load(std::memory_order_acquire);
    if (cells == nullptr) {
      int64_t b = base_.load(std::memory_order_relaxed);
      if (!base_.compare_exchange_strong(b, b + x, std::memory_order_relaxed)) {
        Cell* nc = new Cell[kCells];
        Cell* expected = nullptr;
        if (!cells_.compare_exchange_strong(expected, nc,
                                            std::memory_order_acq_rel)) {
          delete[] nc;
          nc = expected;
        }
        cells = nc;
      } else {
        cells = nullptr;  // counted in the base
      }
    }
    if (cells != nullptr) {  // once cells exist the JDK only uses them
      cells[probe()].v.fetch_add(x, std::memory_order_relaxed);
      if (check <= 1) return;
    }
    if (check < 0) return;
    while (size() >= size_ctl_.load(std::memory_order_acquire)) {
      if (!try_resize()) break;
    }
  }

  // One thread copies every bin into a table of twice the size.  Returns
  // false if another thread is already resizing.
  bool try_resize() {
    bool expected = false;
    if (!resizing_.compare_exchange_strong(expected, true,
                                           std::memory_order_acq_rel))
      return false;
    Table* old = table_.load(std::memory_order_acquire);
    if (size() < size_ctl_.load(std::memory_order_acquire)) {
      resizing_.store(false, std::memory_order_release);
      return false;
    }
    auto t0 = std::chrono::steady_clock::now();
    const size_t n = old->n;
    Table* nt = new Table(n << 1);
    old->next.store(nt, std::memory_order_release);
    for (size_t i = 0; i < n; ++i) {
      Bin& b = old->bins[i];
      // An empty bin is claimed by CAS, since insertions into empty bins do
      // not take the lock (JDK: casTabAt(tab, i, null, fwd)).
      Node* empty = nullptr;
      if (b.head.compare_exchange_strong(empty, moved(),
                                         std::memory_order_acq_rel))
        continue;
      b.lock.lock();
      Node* e = b.head.load(std::memory_order_acquire);
      // Emptied by a remove/clear before we locked: claim it by CAS again
      // (an insertion may still race in); a non-empty head only changes
      // under the lock.
      if (e == nullptr && b.head.compare_exchange_strong(
                              e, moved(), std::memory_order_acq_rel)) {
        b.lock.unlock();
        continue;
      }
      Node *lo_h = nullptr, *lo_t = nullptr, *hi_h = nullptr, *hi_t = nullptr;
      Node* first = e;
      for (; e != nullptr; e = e->next.load(std::memory_order_relaxed)) {
        // Copy, so that readers still walking the old chain stay valid.
        Node* c = new Node(e->key, e->hash,
                           e->val.load(std::memory_order_relaxed));
        if ((static_cast<size_t>(e->hash) & n) == 0) {
          if (lo_t) lo_t->next.store(c, std::memory_order_relaxed);
          else lo_h = c;
          lo_t = c;
        } else {
          if (hi_t) hi_t->next.store(c, std::memory_order_relaxed);
          else hi_h = c;
          hi_t = c;
        }
      }
      nt->bins[i].head.store(lo_h, std::memory_order_release);
      nt->bins[i + n].head.store(hi_h, std::memory_order_release);
      b.head.store(moved(), std::memory_order_release);
      b.lock.unlock();
      for (Node* d = first; d != nullptr;) {
        Node* nx = d->next.load(std::memory_order_relaxed);
        Qsbr::instance().retire(d);
        d = nx;
      }
    }
    table_.store(nt, std::memory_order_release);
    size_ctl_.store(threshold(n << 1), std::memory_order_release);
    Qsbr::instance().retire(old);
    resizes_.fetch_add(1, std::memory_order_relaxed);
    resize_ns_.fetch_add(
        static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - t0)
                .count()),
        std::memory_order_relaxed);
    resizing_.store(false, std::memory_order_release);
    return true;
  }
};

}  // namespace tuskflow

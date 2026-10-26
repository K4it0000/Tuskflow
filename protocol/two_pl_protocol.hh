#pragma once

// TwoPLImpl of the original: two-phase locking over Neo4j's Forseti lock
// manager (forseti.hh).  Used outside the mammoth; not part of paper
// Algorithm 1.  A Forseti client per attempt; locks are held until
// stop_client().

#include <cstdint>
#include <vector>

#include "protocol/forseti.hh"
#include "protocol/protocol.hh"
#include "util/qsbr.hh"

namespace tuskflow {

class TwoPLProtocol final : public Protocol {
public:
  explicit TwoPLProtocol(ForsetiLockManager* manager) : manager_(manager) {}
  ~TwoPLProtocol() override { stop_client(); }  // close()

  void start_client(int64_t tid, int) override {
    client_ = manager_->new_client();
    client_->initialize(tid);
  }
  void stop_client() override {
    if (client_ == nullptr) return;
    client_->close();
    // Other clients may still read it (and its ExclusiveLock) through their
    // wait state; see Qsbr::retire_twice.
    Qsbr::instance().retire_twice(client_);
    client_ = nullptr;
  }
  // Restores the values saved before the in-place writes.
  void abort(std::vector<RWKey*>& ws, GraphStorage* storage) override {
    for (RWKey* w : ws)  // a restore is a physical write (new version)
      if (w->oldVal != nullptr) storage->update_node(*w->oldVal, false);
  }
  // Shared lock (Java prepareRead).
  void reserve_read(std::vector<RWKey*>& rs, RWKey* key, ResourceType type,
                    int64_t id) override {
    try {
      client_->acquire_shared(static_cast<int>(type), id);
    } catch (...) {
      delete key;
      throw;
    }
    *key = RWKey{0, id, type, nullptr, nullptr};
    rs.push_back(key);
  }
  // Exclusive lock (Java prepareUpdate).
  void reserve_write(std::vector<RWKey*>& ws, RWKey* key, ResourceType type,
                     int64_t id) override {
    try {
      client_->acquire_exclusive(static_cast<int>(type), id);
    } catch (...) {
      delete key;
      throw;
    }
    *key = RWKey{0, id, type, nullptr, nullptr};
    ws.push_back(key);
  }
  void reserve_mammoth_write(std::vector<RWKey*>& ws, RWKey* key,
                             ResourceType type, int64_t id,
                             LongRunningState) override {
    reserve_write(ws, key, type, id);
  }
  bool has_conflicts(const std::vector<RWKey*>&,
                     const std::vector<RWKey*>&) override {
    return false;
  }
  void clear() override {}
  bool is_two_pl() const override { return true; }

private:
  ForsetiLockManager* manager_;
  ForsetiClient* client_ = nullptr;
};

}  // namespace tuskflow

#pragma once

// The mammoth of the random-graph workload
// (tasks/storedprocedures/AllNodesLongRunningTx + AbstractLongRunningTransaction):
// writes "Property" = nodeId to every node, in id order, `budget` nodes per
// epoch.  It never aborts: every epoch it installs what it staged and, while
// nodes remain, is scheduled again (Alg.1 l.7-9).

#include <atomic>
#include <chrono>
#include <cstdint>

#include "tx/transaction.hh"

namespace tuskflow {

class AllNodesLongRunningTx : public Tx {
public:
  AllNodesLongRunningTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
                        uint32_t number_of_nodes, uint64_t budget);

  void set_variable_for_completion(std::atomic<int>* has_finished) {
    has_finished_ = has_finished;
  }

private:
  // Alg.1 l.10-12 with a budget (Java runLocally +
  // executeWithoutPartitions): mark the next `budget` nodes VISITED
  // (ReserveMammothWrite) and stage their new values.
  void run_locally() override;
  // Alg.1 l.22-23: the mammoth installs without HasConflicts
  // (AbstractLongRunningTransaction.tryToRunOnDatabase).
  bool try_to_run_on_database() override;
  bool has_more_work() const override { return has_finished_->load() != 0; }
  bool is_mammoth() const override { return true; }
  void prepare_retry() override;
  void on_finished() override;
  // The mammoth always runs in epochs (2PL execution is not ported).
  bool run_on_database_with_retries() override;
  bool logic(bool) override { return false; }

  static uint64_t elapsed_ns(std::chrono::steady_clock::time_point t0);

  uint32_t number_of_nodes_;
  uint64_t budget_;
  std::atomic<int>* has_finished_ = nullptr;
  bool has_more_work_ = true;
  uint64_t last_node_id_ = 0;
  // Phase 2 cost breakdown.
  std::chrono::steady_clock::time_point commit_t0_;
  uint64_t merged_tsc_ = 0;
};

}  // namespace tuskflow

#pragma once

// The mammoth of the random-graph workload
// (tasks/storedprocedures/AllNodesLongRunningTx + AbstractLongRunningTransaction):
// writes "Property" = nodeId to every node, in id order, `budget` nodes per
// epoch.  It never aborts: every epoch it installs what it staged and, while
// nodes remain, is scheduled again (Alg.1 l.7-9).
// AllMessagesLongRunningTx is LDBC Q0 (tasks/storedprocedures/ldbc/): the
// same scan, over the Messages only ("Likes" = nodeId; the budget counts
// Messages).

#include <atomic>
#include <chrono>
#include <cstdint>

#include "storage/ldbc_loader.hh"
#include "tx/transaction.hh"

namespace tuskflow {

class AllNodesLongRunningTx : public Tx {
public:
  AllNodesLongRunningTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
                        uint32_t number_of_nodes, uint64_t budget);

  void set_variable_for_completion(std::atomic<int>* has_finished) {
    has_finished_ = has_finished;
  }
  // labelIndexesUpdated for the tag optimization: the Messages (LDBC Q0),
  // nullptr when every node is written.
  const LdbcGraph* ldbc() const { return ldbc_; }

protected:
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
  const LdbcGraph* ldbc_ = nullptr;  // only Messages are visited when set
  const std::string* property_key_ = &kPropertyKey;
  // Phase 2 cost breakdown.
  std::chrono::steady_clock::time_point commit_t0_;
  uint64_t merged_tsc_ = 0;
};

// AllMessagesLongRunningTx (LDBC Q0)
class AllMessagesLongRunningTx : public AllNodesLongRunningTx {
public:
  AllMessagesLongRunningTx(int64_t tid, GraphStorage* storage,
                           ResultQueue* output, const LdbcGraph* ldbc,
                           uint64_t budget)
      : AllNodesLongRunningTx(tid, storage, output, ldbc->number_of_nodes,
                              budget) {
    ldbc_ = ldbc;
    property_key_ = &kLikesKey;
  }
};

}  // namespace tuskflow

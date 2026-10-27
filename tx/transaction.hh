#pragma once

// AbstractTransaction of the original (tasks/AbstractTransaction.java), the
// part of paper Algorithm 1 that runs a transaction:
//
//   Alg.1 l.1-9    ExecuteTx           Tx::execute_tx
//   Alg.1 l.10-12  RunLocally          Tx::run_locally (per transaction kind)
//   Alg.1 l.21-25  TryToRunOnDatabase  Tx::try_to_run_on_database
//
// Reservations (l.13-20) and HasConflicts (l.26-37) are in protocol/.
// Subclasses: tx/short_tx.hh (short-lived transactions) and
// tx/mammoth_tx.hh (the mammoth).
//
// Outside Algorithm 1, as in the original:
//   - a short transaction that aborted replays its previous read and write
//     sets instead of running its logic again (run_previous_work);
//   - outside the mammoth, transactions run in one step under 2PL
//     (TxType::EXECUTE_AND_COMMIT, run_on_database_with_retries).

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "monitor/ser_log.hh"
#include "protocol/protocol.hh"
#include "storage/entities.hh"
#include "storage/graph_storage.hh"
#include "tx/task.hh"

namespace tuskflow {

// Property names written by the workload.
inline const std::string kPropertyKey = "Property";        // the mammoth
inline const std::string kNewPropertyKey = "New property"; // short writes
inline const std::string kLikesKey = "Likes";              // LDBC Q0

class Tx {
public:
  Tx(int64_t tid, GraphStorage* storage, ResultQueue* output);
  virtual ~Tx();
  Tx(const Tx&) = delete;
  Tx& operator=(const Tx&) = delete;

  // Alg.1 l.1-9 ExecuteTx (Java: run()).  Returns true when the transaction
  // has committed for good (the worker then counts it).
  bool execute_tx();

  void set_transaction_type(TxType type, Protocol* protocol,
                            const EpochContext& ctx);
  void set_epoch(int epoch) { epoch_.store(epoch, std::memory_order_relaxed); }
  void set_start_time(double ms) { start_time_ms_ = ms; }
  bool is_two_pl() const { return protocol_ != nullptr && protocol_->is_two_pl(); }
  // AbstractTransaction.abort(): protocol.abort(writeSet) + clearRWSets().
  void abort_tx();
  int64_t tid() const { return tid_; }
  TxType type() const { return type_; }

protected:
  // --- the steps of Algorithm 1, per transaction kind --------------------
  // Alg.1 l.10-12: read the latest snapshot, execute locally, reserve the
  // reads and writes.
  virtual void run_locally() = 0;
  // Alg.1 l.21-25: install the writes unless HasConflicts (the mammoth
  // overrides it: it always installs).
  virtual bool try_to_run_on_database();
  // Alg.1 l.7: the mammoth has more work after this epoch.
  virtual bool has_more_work() const { return false; }
  virtual bool is_mammoth() const { return false; }
  // Before the transaction is scheduled again for the next epoch (l.8-9).
  virtual void prepare_retry();
  // After the transaction has committed for good.
  virtual void on_finished() {}
  // The 2PL mode outside Algorithm 1 (Java runOnDatabaseWithRetries).
  virtual bool run_on_database_with_retries();
  // The transaction's logic (Java readTxLogic / mixedTxLogic): `local` is
  // true in epoch mode (stage writes), false under 2PL (write in place).
  virtual bool logic(bool local) = 0;

  // --- reads and writes, as the logic calls them --------------------------
  Node* read_node(uint32_t node_id);
  Relationship* read_relationship(uint32_t rel_id);
  const IntList* read_out_neighbourhood(uint32_t node_id);
  const IntList* read_in_neighbourhood(uint32_t node_id);
  Node* prepare_update_node(uint32_t node_id);

  // Replay of an aborted attempt (not in Algorithm 1; Java
  // hasPreviousWork / runPreviousWork): re-reserve the previous reads and
  // writes, with the values staged then, without reading storage again.
  bool has_previous_work() const;
  bool run_previous_work();

  // --- scheduling (Java createCommitTask / createExecuteLocallyTask) -----
  void create_commit_task(bool is_long_running);
  void create_execute_locally_task(bool is_long_running);
  void create_task(TxType type, ConcurrentQueue<Task>* queue,
                   bool is_long_running);

  // --- objects the JVM would reclaim -------------------------------------
  static RWKey* new_key() { return new RWKey; }
  static void delete_keys(std::vector<RWKey*>& keys);
  void release_allocations();

  // Serializability log: node versions read and written by the
  // attempt that commits.  A replay does not read storage again, so the
  // reads of the attempt it replays are kept.
  void ser_fresh_attempt() {
    ser_reads_.clear();
    ser_writes_.clear();
  }

  int64_t tid_;
  GraphStorage* storage_;
  ResultQueue* output_;
  Protocol* protocol_ = nullptr;
  double start_time_ms_ = 0.0;
  std::vector<RWKey*> read_set_, write_set_;            // the RWSet
  std::vector<RWKey*> prev_read_set_, prev_write_set_;  // for the replay
  TxType type_ = TxType::EXECUTE_AND_COMMIT;
  std::atomic<int> epoch_{-1};
  bool track_accesses_;
  EpochContext ctx_{};
  // RWKeys are owned by the four sets (each key is in exactly one of them);
  // the keys of an older attempt are freed when the sets are replaced on the
  // next abort.  Local node copies live until the transaction ends (the
  // mammoth frees them after each epoch).
  std::vector<Node*> copies_;
  SerPairs ser_reads_, ser_writes_;
};

}  // namespace tuskflow

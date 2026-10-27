#include "tx/transaction.hh"

#include <atomic>

#include "monitor/counters.hh"
#include "protocol/forseti.hh"
#include "util/thread_random.hh"

namespace tuskflow {

Tx::Tx(int64_t tid, GraphStorage* storage, ResultQueue* output)
    : tid_(tid), storage_(storage), output_(output),
      track_accesses_(track_access_draw()) {}

Tx::~Tx() {
  delete protocol_;
  release_allocations();
}

// Alg.1 l.1-9 ExecuteTx.  The original spreads this over each transaction's
// run(), runLocally() and tryToRunOnDatabase(); the steps and their order
// are the same.
bool Tx::execute_tx() {
  switch (type_) {
    case TxType::EXECUTE_AND_COMMIT:  // 2PL, outside Algorithm 1
      return run_on_database_with_retries();

    case TxType::EXECUTE_LOCALLY:  // l.2-5
      run_locally();                       // l.3
      create_commit_task(is_mammoth());    // l.4-5: RUN_ON_DB, commit phase
      std::atomic_thread_fence(std::memory_order_seq_cst);
      ctx_.phase_one_counter->count_down();
      return false;

    case TxType::COMMIT: {  // l.6-9
      bool committed = try_to_run_on_database();  // l.7
      bool again = !committed || has_more_work();  // l.7
      if (again) {                                 // l.8-9
        ctx_.aborted_counter->fetch_add(1);  // the mammoth too ("technically
                                             // this is not aborted")
        prepare_retry();
        create_execute_locally_task(is_mammoth());
      } else {
        on_finished();
      }
      std::atomic_thread_fence(std::memory_order_seq_cst);
      ctx_.phase_two_counter->count_down();
      return !again;
    }
  }
  return false;
}

// Alg.1 l.21-25 for a short-lived transaction (Java
// AbstractTransaction.tryToRunOnDatabase).
bool Tx::try_to_run_on_database() {
  if (protocol_->has_conflicts(read_set_, write_set_)) return false;  // l.22
  for (RWKey* w : write_set_) {  // l.23 (TRACK_ALL_ACCESSES_TOGETHER = true)
    uint32_t v = storage_->update_node(*w->newVal, track_accesses_);
    if (SerLog::on()) ser_writes_.push_back({static_cast<uint32_t>(w->id), v});
  }
  if (SerLog::on()) {
    SerLog::emit(tid_, kSerEpoch, ser_reads_, ser_writes_);
    ser_writes_.clear();
  }
  if (output_ != nullptr) output_->add(new Result{true});
  return true;  // l.24
}

// An aborted short transaction keeps this attempt for the replay
// (prevReadSet.set(new ArrayList<>(readSet)): the attempt before becomes
// garbage).
void Tx::prepare_retry() {
  delete_keys(prev_read_set_);
  delete_keys(prev_write_set_);
  prev_read_set_.swap(read_set_);
  prev_write_set_.swap(write_set_);
  protocol_->clear();
  g_aborts.v.fetch_add(1, std::memory_order_relaxed);
}

// runOnDatabaseWithRetries (2PL): a deadlock aborts (restoring the saved
// values) and re-runs the logic, up to TX_RETRIES = 10 attempts.
bool Tx::run_on_database_with_retries() {
  static constexpr int kTxRetries = 10;
  int count = 0;
  bool succeeded = false;
  try {
    for (;;) {
      try {
        protocol_->start_client(tid_, 0);
        succeeded = logic(false);
        protocol_->stop_client();
        if (SerLog::on() && succeeded)
          SerLog::emit(tid_, kSerTwoPL, ser_reads_, ser_writes_);
        break;
      } catch (const DeadlockDetectedException&) {
        abort_tx();
        protocol_->stop_client();
        g_2pl_deadlock_retries.v.fetch_add(1, std::memory_order_relaxed);
        if (++count == kTxRetries) throw;
      }
    }
  } catch (...) {
    abort_tx();
    g_2pl_failed.v.fetch_add(1, std::memory_order_relaxed);
    if (output_ != nullptr) output_->add(new Result{succeeded});
    throw;
  }
  if (output_ != nullptr) output_->add(new Result{succeeded});
  return succeeded;
}

void Tx::set_transaction_type(TxType type, Protocol* protocol,
                              const EpochContext& ctx) {
  type_ = type;
  if (protocol != protocol_) {  // the old protocol object becomes garbage
    delete protocol_;
    protocol_ = protocol;
  }
  ctx_ = ctx;
}

void Tx::abort_tx() {
  protocol_->abort(write_set_, storage_);
  delete_keys(read_set_);
  delete_keys(write_set_);
}

// --- reads and writes (Java readNode / readRelationship /
// readOutNeighbourhood / prepareUpdateNode): reserve, then read storage ----

Node* Tx::read_node(uint32_t node_id) {
  protocol_->reserve_read(read_set_, new_key(), ResourceType::NODE, node_id);
  Node* n = storage_->get_node(node_id, track_accesses_);
  if (SerLog::on()) ser_reads_.push_back({node_id, storage_->version(node_id)});
  return n;
}

Relationship* Tx::read_relationship(uint32_t rel_id) {
  protocol_->reserve_read(read_set_, new_key(), ResourceType::RELATIONSHIP,
                          rel_id);
  return storage_->get_relationship(rel_id, track_accesses_);
}

const IntList* Tx::read_out_neighbourhood(uint32_t node_id) {
  protocol_->reserve_read(read_set_, new_key(), ResourceType::NODE_RELATIONSHIPS,
                          node_id);
  return storage_->get_out_relationships(node_id);
}

const IntList* Tx::read_in_neighbourhood(uint32_t node_id) {
  protocol_->reserve_read(read_set_, new_key(), ResourceType::NODE_RELATIONSHIPS,
                          node_id);
  return storage_->get_in_relationships(node_id);
}

Node* Tx::prepare_update_node(uint32_t node_id) {
  protocol_->reserve_write(write_set_, new_key(), ResourceType::NODE, node_id);
  return storage_->get_node(node_id, track_accesses_);
}

bool Tx::has_previous_work() const {
  return !is_two_pl() && (!prev_read_set_.empty() || !prev_write_set_.empty());
}

bool Tx::run_previous_work() {
  if (!has_previous_work()) return false;
  for (const RWKey* e : prev_read_set_)
    protocol_->reserve_read(read_set_, new_key(), e->type, e->id);
  for (const RWKey* e : prev_write_set_) {
    protocol_->reserve_write(write_set_, new_key(), e->type, e->id);
    Protocol::update_last_write_set_entry(write_set_, nullptr, e->newVal);
  }
  return true;
}

// --- scheduling -----------------------------------------------------------

void Tx::create_commit_task(bool is_long_running) {
  create_task(TxType::COMMIT, ctx_.phase_two_queue, is_long_running);
}

void Tx::create_execute_locally_task(bool is_long_running) {
  create_task(TxType::EXECUTE_LOCALLY, ctx_.phase_one_queue, is_long_running);
}

void Tx::create_task(TxType type, ConcurrentQueue<Task>* queue,
                     bool is_long_running) {
  Task* task = TaskPool::new_instance(static_cast<int>(tid_), is_long_running);
  task->start_time_ms = start_time_ms_;
  task->tx = this;
  set_transaction_type(type, protocol_, ctx_);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  queue->add(task);
  std::atomic_thread_fence(std::memory_order_seq_cst);
}

// --- memory -----------------------------------------------------------------

void Tx::delete_keys(std::vector<RWKey*>& keys) {
  for (RWKey* k : keys) delete k;
  keys.clear();
}

void Tx::release_allocations() {
  delete_keys(read_set_);
  delete_keys(write_set_);
  delete_keys(prev_read_set_);
  delete_keys(prev_write_set_);
  for (Node* n : copies_) delete n;
  copies_.clear();
}

}  // namespace tuskflow

#include "tx/mammoth_tx.hh"

#include <x86intrin.h>

#include <cinttypes>
#include <cstdio>
#include <cstdlib>

#include "config.hh"
#include "monitor/counters.hh"

namespace tuskflow {

AllNodesLongRunningTx::AllNodesLongRunningTx(int64_t tid, GraphStorage* storage,
                                             ResultQueue* output,
                                             uint32_t number_of_nodes,
                                             uint64_t budget)
    : Tx(tid, storage, output), number_of_nodes_(number_of_nodes),
      budget_(budget) {}

void AllNodesLongRunningTx::run_locally() {
  auto t0 = std::chrono::steady_clock::now();
  protocol_->start_client(tid_, epoch_.load(std::memory_order_relaxed));
  // executeWithoutPartitions
  int64_t budget = static_cast<int64_t>(budget_);
  uint64_t node_id = last_node_id_;
  const bool bd = FLAGS_lr_breakdown;
  uint64_t c_res = 0, c_get = 0, c_copy = 0, c_rest = 0;
  while (budget > 0 && node_id < number_of_nodes_) {
    uint64_t t_0 = bd ? __rdtsc() : 0;
    protocol_->reserve_mammoth_write(write_set_, new_key(), ResourceType::NODE,
                                     static_cast<int64_t>(node_id),
                                     LongRunningState::VISITED);
    uint64_t t_1 = bd ? __rdtsc() : 0;
    Node* node = storage_->get_node(static_cast<uint32_t>(node_id));
    if (node == nullptr)
      std::printf("[DBG] node %" PRIu64 " not found\n", node_id);
    uint64_t t_2 = bd ? __rdtsc() : 0;
    uint32_t ver_read = 0;
    Node* new_val = node->copy(storage_->version_ptr(static_cast<uint32_t>(node_id)),
                               &ver_read);
    if (SerLog::on())
      ser_reads_.push_back({static_cast<uint32_t>(node_id), ver_read});
    uint64_t t_3 = bd ? __rdtsc() : 0;
    copies_.push_back(new_val);
    new_val->add_property(kPropertyKey, static_cast<int64_t>(node_id));
    Protocol::update_last_write_set_entry(write_set_, nullptr, new_val);
    if (bd) {
      uint64_t t_4 = __rdtsc();
      c_res += t_1 - t_0;
      c_get += t_2 - t_1;
      c_copy += t_3 - t_2;
      c_rest += t_4 - t_3;
    }
    ++node_id;
    --budget;
  }
  if (bd) {
    g_lr_cyc_reserve.v.fetch_add(c_res);
    g_lr_cyc_get.v.fetch_add(c_get);
    g_lr_cyc_copy.v.fetch_add(c_copy);
    g_lr_cyc_rest.v.fetch_add(c_rest);
  }
  last_node_id_ = node_id;
  if (node_id >= number_of_nodes_ && has_more_work_) {
    has_finished_->fetch_sub(1);
    has_more_work_ = false;
  }
  g_lr_phase1_ns.v.fetch_add(elapsed_ns(t0), std::memory_order_relaxed);
}

bool AllNodesLongRunningTx::try_to_run_on_database() {
  commit_t0_ = std::chrono::steady_clock::now();
  uint64_t m0 = __rdtsc();
  for (RWKey* w : write_set_) {  // "Writes must always succeed"
    uint32_t v = storage_->update_node(*w->newVal);
    if (SerLog::on()) ser_writes_.push_back({static_cast<uint32_t>(w->id), v});
  }
  if (SerLog::on()) {
    SerLog::emit(tid_, kSerMammoth, ser_reads_, ser_writes_);
    ser_fresh_attempt();
  }
  merged_tsc_ = __rdtsc();
  g_lr_cyc_merge.v.fetch_add(merged_tsc_ - m0);
  return true;
}

// More nodes remain: readSet/writeSet.clear(); the JVM collects the copies of
// this epoch.
void AllNodesLongRunningTx::prepare_retry() {
  release_allocations();
  g_lr_cyc_free.v.fetch_add(__rdtsc() - merged_tsc_);
  g_lr_phase2_ns.v.fetch_add(elapsed_ns(commit_t0_), std::memory_order_relaxed);
}

void AllNodesLongRunningTx::on_finished() {
  g_lr_phase2_ns.v.fetch_add(elapsed_ns(commit_t0_), std::memory_order_relaxed);
  if (output_ != nullptr) output_->add(new Result{true});
}

bool AllNodesLongRunningTx::run_on_database_with_retries() {
  std::fprintf(stderr, "EXECUTE_AND_COMMIT (2PL) is not implemented\n");
  std::abort();
}

uint64_t AllNodesLongRunningTx::elapsed_ns(std::chrono::steady_clock::time_point t0) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - t0)
          .count());
}

}  // namespace tuskflow

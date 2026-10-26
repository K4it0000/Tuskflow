#include "tx/task.hh"

#include <cstdio>
#include <exception>

#include "monitor/counters.hh"
#include "tx/transaction.hh"

namespace tuskflow {

void Task::free_task() {
  task_id = -1;
  long_running = false;
  tx = nullptr;
  start_time_ms = 0.0;
  TaskPool::free(this);
}

bool Task::run() {
  if (tx == nullptr) return true;
  int mode = g_execution_mode->load(std::memory_order_acquire);
  if (tx->type() == TxType::COMMIT && mode != 2)
    g_diag_commit_outside_p2.v.fetch_add(1, std::memory_order_relaxed);
  if (tx->type() == TxType::EXECUTE_LOCALLY && mode != 1)
    g_diag_local_outside_p1.v.fetch_add(1, std::memory_order_relaxed);
  try {
    return tx->execute_tx();
  } catch (const std::exception& e) {
    // Task.run: transaction.abort(); e.printStackTrace(); the result stays
    // true, so the worker counts the task as committed.
    tx->abort_tx();
    std::printf("[DBG] tx %d failed: %s\n", task_id, e.what());
    return true;
  }
}

}  // namespace tuskflow

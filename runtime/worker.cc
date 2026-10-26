#include "runtime/worker.hh"

#include "monitor/counters.hh"
#include "tx/task.hh"
#include "tx/transaction.hh"
#include "util/concurrency.hh"
#include "util/qsbr.hh"

namespace tuskflow {

void worker_main(size_t worker_id, QueryManager& manager, Metrics& metrics,
                 std::atomic<bool>& db_running) {
  Qsbr& qsbr = Qsbr::instance();
  while (db_running.load()) {
    Task* task = nullptr;
    for (;;) {
      qsbr.quiescent();  // no shared pointer is held between tasks
      if ((task = manager.get_next()) != nullptr) break;
      if (!db_running.load()) {
        qsbr.offline();
        return;
      }
      park_nanos_1();
    }
    const bool two_pl = task->tx != nullptr &&
                        task->tx->type() == TxType::EXECUTE_AND_COMMIT;
    int epoch0 = 0;
    if (two_pl) {
      g_inflight_2pl.v.fetch_add(1);
      epoch0 = manager.epoch();
      if (manager.mode() != 0)
        g_diag_2pl_overlap.v.fetch_add(1, std::memory_order_relaxed);
    }
    bool has_run = task->run();
    if (two_pl) {
      if (manager.epoch() != epoch0)  // an epoch ran meanwhile
        g_diag_2pl_overlap.v.fetch_add(1, std::memory_order_relaxed);
      g_inflight_2pl.v.fetch_sub(1);
    }
    if (has_run) {
      metrics.add_committed(worker_id);
      if (two_pl) g_commits_2pl.v.fetch_add(1, std::memory_order_relaxed);
      double end = now_millis();
      metrics.add_latency(worker_id, end, end - task->start_time_ms);
    }
    // The transaction is finished (garbage in Java) after a 2PL run or a
    // successful commit; otherwise it was re-queued.
    if (has_run || two_pl) delete task->tx;
    task->free_task();
  }
}

}  // namespace tuskflow

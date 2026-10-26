#pragma once

// Task and TaskPool of the original (tasks/): a task carries a transaction
// through the queues; the transaction's type says which step runs next.

#include <atomic>

#include "util/concurrency.hh"

namespace tuskflow {

struct Result {  // database/Result
  bool succeeded;
};
using ResultQueue = ConcurrentQueue<Result>;

// TransactionType.  Paper Algorithm 1 names: EXECUTE_LOCALLY (execution
// phase) and RUN_ON_DB (= COMMIT, commit phase).  EXECUTE_AND_COMMIT is the
// one-step 2PL execution used outside the mammoth.
enum class TxType { EXECUTE_AND_COMMIT, EXECUTE_LOCALLY, COMMIT };

class Tx;

struct Task {
  int task_id = -1;
  bool long_running = false;
  double start_time_ms = 0.0;
  Tx* tx = nullptr;
  bool run();  // Task.run(): Tx::execute_tx (Alg.1 ExecuteTx)
  void free_task();
};

class TaskPool {
public:
  static void init() {
    for (int i = 0; i < 1000; ++i) pool().add(new Task());
    count().store(1000);
  }
  static Task* new_instance(int task_id, bool long_running) {
    Task* t = pool().poll();
    if (t == nullptr) {
      t = new Task();
      count().fetch_add(1, std::memory_order_relaxed);
    }
    t->task_id = task_id;
    t->long_running = long_running;
    return t;
  }
  static void free(Task* t) { pool().add(t); }
  static std::atomic<int>& count() {
    static std::atomic<int> c{0};
    return c;
  }

private:
  static ConcurrentQueue<Task>& pool() {
    static ConcurrentQueue<Task> p;
    return p;
  }
};

// Queues and counters a transaction needs in epoch mode
// (AbstractTransaction.setTransactionType arguments).
struct EpochContext {
  ConcurrentQueue<Task>* phase_one_queue;
  ConcurrentQueue<Task>* phase_two_queue;
  std::atomic<int>* aborted_counter;
  CountDownLatch* phase_one_counter;
  CountDownLatch* phase_two_counter;
};

}  // namespace tuskflow

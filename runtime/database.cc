#include "runtime/database.hh"

#include <chrono>
#include <cstdio>
#include <thread>

#include "config.hh"
#include "protocol/two_pl_protocol.hh"
#include "tx/mammoth_tx.hh"
#include "util/concurrency.hh"

namespace tuskflow {

void Database::submit_query(bool is_read, bool long_running, ResultQueue* output) {
  int task_id = last_tx_id_.fetch_add(1) + 1;
  Task* task = TaskPool::new_instance(task_id, long_running);
  if (long_running) {
    if (cfg_.ldbc != nullptr)  // LDBC_QUERY_TYPE = 0
      task->tx = new AllMessagesLongRunningTx(task_id, storage_, output,
                                              cfg_.ldbc, FLAGS_lr_budget);
    else
      task->tx = new AllNodesLongRunningTx(task_id, storage_, output,
                                           cfg_.number_of_nodes,
                                           FLAGS_lr_budget);
  } else if (cfg_.ldbc != nullptr) {  // USE_TRAVERSALS = true
    if (is_read)
      task->tx = new FriendMessagesReadTx(task_id, storage_, output, cfg_);
    else
      task->tx = new FriendMessagesUpdateTx(task_id, storage_, output, cfg_);
  } else if (is_read) {
    task->tx = new ReadTx(task_id, storage_, output, cfg_);
  } else {
    task->tx = new MixedTx(task_id, storage_, output, cfg_);
  }
  // CypherParser creates every transaction with `new TwoPLImpl()`.
  task->tx->set_transaction_type(TxType::EXECUTE_AND_COMMIT,
                                 new TwoPLProtocol(locks_), EpochContext{});
  task->start_time_ms = now_millis();
  manager_->submit(task);
}

// PrototypeClient with USE_ASYNC_CLIENTS = true.
void client_main(Database* db, std::atomic<bool>* bench_running,
                 int read_ratio, uint64_t txs_per_second) {
  JavaRandom rnd(42);  // every client uses new Random(42L)
  // The connection outlives the client thread: transactions still in flight
  // add their results to it (kept alive by the GC in the original).
  ResultQueue& results = *new ResultQueue();
  while (!bench_running->load())
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  uint64_t throughput_counter = 0;
  auto start = std::chrono::steady_clock::now();
  while (bench_running->load()) {
    bool is_read = rnd.next_int(100) < read_ratio;  // randInt(rand, 0, 99)
    db->submit_query(is_read, false, &results);
    ++throughput_counter;
    if (txs_per_second > 0 && throughput_counter == txs_per_second) {
      auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start)
                    .count();
      if (dt < 1000)
        std::this_thread::sleep_for(std::chrono::milliseconds(1000 - dt));
      start = std::chrono::steady_clock::now();
      throughput_counter = 0;
    }
    while (Result* r = results.poll()) delete r;  // consumeAsync
  }
}

// PrototypeLongRunningClient
void long_running_client_main(Database* db, std::atomic<bool>* bench_running,
                              uint64_t delay_ms) {
  ResultQueue& results = *new ResultQueue();
  while (!bench_running->load())
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto start = std::chrono::steady_clock::now();
  while (bench_running->load()) {
    auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - start)
                  .count();
    if (static_cast<uint64_t>(dt) >= delay_ms) {
      start = std::chrono::steady_clock::now();
      std::printf("[DBG] Starting long-running tx.\n");
      std::fflush(stdout);
      db->submit_query(false, true, &results);
      while (results.empty()) park_nanos_1();
      delete results.poll();
      double ms = std::chrono::duration<double, std::milli>(
                      std::chrono::steady_clock::now() - start)
                      .count();
      std::printf("[DBG] Long-running tx has ended in %f millis.\n", ms);
      std::fflush(stdout);
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}

}  // namespace tuskflow

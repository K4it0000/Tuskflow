#pragma once

// LongRunningQueryManager of the original (runtime/): the scheduler around
// paper Algorithm 1.  Outside the mammoth, tasks run in one step under 2PL
// (execution mode 0).  When a mammoth arrives, the manager switches to
// epochs: each epoch moves up to EPOCH_SIZE pending tasks to the running
// queue, lets the workers run Phase 1 (execution, mode 1) and then Phase 2
// (commit, mode 2), separated by count-down barriers.  When the mammoth has
// finished, the remaining tasks go back to 2PL.
//
// Two races of the original are kept by default; --fix_getnext_race and
// --fix_mode_switch_race remove them.

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "protocol/forseti.hh"
#include "protocol/reservation_table.hh"
#include "tx/task.hh"
#include "util/concurrency.hh"

namespace tuskflow {

struct EpochRecord {  // --epoch_log
  int epoch;
  double t_ms;
  int count;
  double phase1_ms;
  double phase2_ms;
  bool lr_active;
};

class QueryManager {
public:
  QueryManager(ReservationTable* table, ForsetiLockManager* locks,
               std::atomic<bool>* running, uint64_t epoch_size);
  ~QueryManager() { join_background(); }
  void join_background();

  // Clients hand in transactions (submit); workers take them (getNext).
  void submit(Task* task);
  Task* get_next();
  // The manager's own thread (LongRunningQueryManager.run()).
  void run();

  // Monitoring.
  std::string queues_string() const;
  int epoch() const { return epoch_.load(); }
  int mode() const { return execution_mode_.load(); }
  std::atomic<int>* execution_mode() { return &execution_mode_; }
  bool lr_active() const { return lr_active_.load(); }
  double lr_start_ms() const { return lr_start_ms_.load(); }
  double lr_end_ms() const { return lr_end_ms_.load(); }
  int lr_start_epoch() const { return lr_start_epoch_.load(); }
  int lr_end_epoch() const { return lr_end_epoch_.load(); }
  double clear_ms() const { return clear_ms_.load(); }
  const std::vector<EpochRecord>& epoch_log() const { return epoch_log_; }
  const std::vector<double>& phase_latency(int p) const { return phase_lat_[p]; }

private:
  // scheduleInEpochs(): run epochs until the mammoth has finished.
  void schedule_in_epochs();
  // One epoch: Phase 1 and Phase 2 of Algorithm 1 for the scheduled tasks.
  void run_epoch();
  int schedule_next_epoch();
  void schedule_long_running_task();
  void check_long_running_tasks();
  // resetExecutionMode(): back to 2PL after the mammoth.
  void reset_execution_mode();
  void change_execution_mode(Task* task, int epoch);  // to the epoch protocol
  void convert_to_two_pl(Task* t);
  bool await_phase(CountDownLatch& counter);
  EpochContext context();

  ReservationTable* table_;
  ForsetiLockManager* locks_;
  std::atomic<bool>* running_;
  int batch_size_;  // EPOCH_SIZE
  ConcurrentQueue<Task> pending_queries_;
  ConcurrentQueue<Task> running_queries_;
  ConcurrentQueue<Task> running_queries2_;  // _runningQueries (phase two)
  ConcurrentQueue<Task> long_running_queries_;
  // -1: stop, 0: regular (2PL), 1: phase one, 2: phase two
  // Padded* in the original: each hot counter on its own cache line.
  alignas(64) std::atomic<int> execution_mode_{0};
  alignas(64) std::atomic<bool> set_queries_{true};
  std::thread clear_thread_;
  CountDownLatch phase_one_counter_;
  CountDownLatch phase_two_counter_;
  alignas(64) std::atomic<int> aborted_counter_{0};
  alignas(64) std::atomic<int> work_counter_{0};
  alignas(64) std::atomic<int> has_long_running_finished_{0};
  alignas(64) std::atomic<int> epoch_{0};
  std::atomic<bool> lr_active_{false};
  std::atomic<double> lr_start_ms_{0}, lr_end_ms_{0};
  std::atomic<int> lr_start_epoch_{-1}, lr_end_epoch_{-1};
  std::atomic<double> clear_ms_{0};
  std::vector<EpochRecord> epoch_log_;
  std::vector<double> phase_lat_[2];
};

}  // namespace tuskflow

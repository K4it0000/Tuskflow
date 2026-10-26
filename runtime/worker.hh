#pragma once

// Worker of the original (runtime/Worker): takes tasks from the manager and
// runs them, counting committed transactions and their latency.

#include <atomic>
#include <cstddef>

#include "monitor/metrics.hh"
#include "runtime/query_manager.hh"

namespace tuskflow {

void worker_main(size_t worker_id, QueryManager& manager, Metrics& metrics,
                 std::atomic<bool>& db_running);

}  // namespace tuskflow

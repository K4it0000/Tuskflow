#pragma once

// Database of the original (database/Database + CypherParser.parseRandom):
// turns a client request into a transaction and hands it to the manager.
// Also the clients of benchmarks/workers/applicationclients/.

#include <atomic>
#include <cstdint>

#include "protocol/forseti.hh"
#include "runtime/query_manager.hh"
#include "storage/graph_storage.hh"
#include "tx/short_tx.hh"
#include "tx/task.hh"

namespace tuskflow {

class Database {
public:
  Database(GraphStorage* storage, QueryManager* manager,
           ForsetiLockManager* locks, const ShortTxConfig& cfg)
      : storage_(storage), manager_(manager), locks_(locks), cfg_(cfg) {}

  // Database.submitQuery + CypherParser.parseRandom: a read (ReadTx), a write
  // (MixedTx) or the mammoth (AllNodesLongRunningTx), created for 2PL.  For
  // LDBC: FriendMessagesReadTx / FriendMessagesUpdateTx and
  // AllMessagesLongRunningTx.
  void submit_query(bool is_read, bool long_running, ResultQueue* output);

private:
  GraphStorage* storage_;
  QueryManager* manager_;
  ForsetiLockManager* locks_;
  ShortTxConfig cfg_;
  std::atomic<int> last_tx_id_{-1};
};

// PrototypeClient (USE_ASYNC_CLIENTS = true): submits `txs_per_second`
// transactions, then waits for the rest of the second.
void client_main(Database* db, std::atomic<bool>* bench_running,
                 int read_ratio, uint64_t txs_per_second);
// PrototypeLongRunningClient: submits one mammoth after `delay_ms`.
void long_running_client_main(Database* db, std::atomic<bool>* bench_running,
                              uint64_t delay_ms);

}  // namespace tuskflow

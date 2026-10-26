#pragma once

// Short-lived transactions of the random-graph workload
// (tasks/storedprocedures/RandomNodeTraversalReadsTx / ...MixedTx):
// from a Zipf-chosen start node, hop (hops - 1) times, then read
// (ReadTx) or write (MixedTx) (ops - hops) Zipf-chosen out-neighbours.

#include <cstdint>
#include <vector>

#include "util/zipf.hh"
#include "tx/transaction.hh"

namespace tuskflow {

struct ShortTxConfig {
  uint32_t number_of_nodes;
  const std::vector<int32_t>* mappings;  // DBConfig.nodeMappingsForZipf
  int ops;
  int hops;
  bool use_zipf;
};

class ShortTx : public Tx {
public:
  ShortTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
          const ShortTxConfig& cfg);

protected:
  // Alg.1 l.10-12 (Java runLocally): run the logic in epoch mode; the
  // reservations are made as it reads and writes.
  void run_locally() override;

  uint32_t get_next_id();
  uint32_t pick_index(const ZipfRejectionInversion* zipf_n, size_t size);
  uint32_t single_read_op(const ZipfRejectionInversion* zipf_n,
                          const IntList& out);
  uint32_t get_next_neighbour(uint32_t node_id);

  ShortTxConfig cfg_;
  ZipfRejectionInversion zipf_;  // over the start nodes
};

// RandomNodeTraversalReadsTx
class ReadTx : public ShortTx {
public:
  using ShortTx::ShortTx;

private:
  bool logic(bool local) override;  // readTxLogic
  bool read_neighbours(uint32_t node_id);
};

// RandomNodeTraversalMixedTx
class MixedTx : public ShortTx {
public:
  using ShortTx::ShortTx;

private:
  bool logic(bool local) override;  // mixedTxLogic(local)
  bool updates_neighbours_of_node(uint32_t node_id, bool local);
  bool update_node(uint32_t node_id, bool local);
};

}  // namespace tuskflow

#pragma once

// RandomLoader + BenchmarkUtils.populateDatabase of the original for the
// random graph, and initializeMappings (the shuffled start-node mapping).

#include <cstdint>
#include <vector>

#include "storage/graph_storage.hh"

namespace tuskflow {

// Loads `nodes` nodes ("Property" = id) and `rels` relationships into
// `storage`; returns DBConfig.nodeMappingsForZipf.
std::vector<int32_t> load_random_graph(GraphStorage* storage, uint32_t nodes,
                                       uint64_t rels, uint64_t seed);

// Utils.shuffleArray over the identity [0, n): new Random(42L).
std::vector<int32_t> shuffled_identity(uint32_t n);

}  // namespace tuskflow

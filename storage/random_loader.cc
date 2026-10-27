#include "storage/random_loader.hh"

#include <utility>

#include "util/zipf.hh"
#include "util/random.hh"
#include "tx/transaction.hh"                // kPropertyKey
#include "util/concurrency.hh"              // JavaRandom

namespace tuskflow {

std::vector<int32_t> load_random_graph(GraphStorage* storage, uint32_t nodes,
                                       uint64_t rels, uint64_t seed) {
  const uint32_t n = nodes;
  for (uint32_t i = 0; i < n; ++i) {
    Node* node = new Node(i);
    node->add_property(kPropertyKey, i);
    storage->unsafe_insert_node(node);
  }
  {
    Xoroshiro128Plus rnd;
    rnd.s[0] = rnd.splitMix64(seed);
    rnd.s[1] = rnd.splitMix64(rnd.s[0]);
    ZipfRejectionInversion zipf(n, 1.0);
    auto coin = [&rnd] { return (rnd.next() >> 63) != 0; };
    uint64_t count = 0;
    uint32_t from = 0;
    while (count < rels) {
      bool create = coin();
      while (create && count < rels) {
        uint64_t to = 0;
        do { to = zipf(rnd); } while (to >= n);  // deviation: redraw n
        storage->unsafe_insert_relationship(
            new Relationship(static_cast<int64_t>(count), from,
                             static_cast<int64_t>(to), 42));
        ++count;
        create = coin();
      }
      from = (from + 1 == n) ? 0 : from + 1;
    }
  }
  return shuffled_identity(n);  // initializeMappings()
}

std::vector<int32_t> shuffled_identity(uint32_t n) {
  std::vector<int32_t> mappings(n);
  for (uint32_t i = 0; i < n; ++i) mappings[i] = static_cast<int32_t>(i);
  JavaRandom rnd(42);
  for (int32_t i = static_cast<int32_t>(n) - 1; i > 0; --i) {
    int32_t index = rnd.next_int(i + 1);
    std::swap(mappings[index], mappings[i]);
  }
  return mappings;
}

}  // namespace tuskflow

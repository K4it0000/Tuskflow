// Serializability check for TuskFlow runs.
//
// Input: logs written by tuskflow.exe --ser_log: int32 little-endian words,
// one record per committed transaction (the mammoth: one per epoch):
//   tid, kind (0 epoch, 1 2PL, 2 mammoth), nreads, nwrites,
//   (node, version) * nreads, (node, version) * nwrites.
// Every physical write of a node increments its version; a read records the
// version it saw.  Aborted attempts are not logged, so versions they created
// (2PL writes and their restores) are gaps; a read of such a version is
// attributed to the latest committed write at or below it.
//
// Output:
//   1. relation of each committed short transaction to the mammoth, from the
//      versions it read (as the existing Java oracle's TXORDER counters):
//      before-only / after-only / unsafe (read both a pre- and a
//      post-mammoth version);
//   2. the dependency graph over all committed transactions (wr, ww, rw edges
//      on node versions) and its cycles (strongly connected components).
//      Committed transactions are conflict-serializable iff there is none.

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <unordered_map>
#include <vector>

namespace {

struct Access {
  uint32_t node;
  uint32_t version;
  uint32_t tx;
};

struct Tx {
  int32_t tid;
  int32_t kind;
};

bool read_file(const char* path, std::vector<int32_t>& words) {
  FILE* f = std::fopen(path, "rb");
  if (f == nullptr) return false;
  std::fseek(f, 0, SEEK_END);
  long bytes = std::ftell(f);
  std::fseek(f, 0, SEEK_SET);
  size_t old = words.size();
  words.resize(old + static_cast<size_t>(bytes) / 4);
  size_t got = std::fread(words.data() + old, 4, words.size() - old, f);
  std::fclose(f);
  return got == words.size() - old;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s <ser_log> [more logs]\n", argv[0]);
    return 2;
  }
  std::vector<int32_t> w;
  for (int i = 1; i < argc; ++i)
    if (!read_file(argv[i], w)) {
      std::fprintf(stderr, "cannot read %s\n", argv[i]);
      return 2;
    }

  std::vector<Tx> txs;
  std::unordered_map<int32_t, uint32_t> index;  // tid -> tx
  std::vector<Access> reads, writes;
  uint64_t records[3] = {0, 0, 0};
  for (size_t p = 0; p < w.size();) {
    if (p + 4 > w.size()) {
      std::fprintf(stderr, "truncated record at word %zu\n", p);
      return 2;
    }
    int32_t tid = w[p], kind = w[p + 1], nr = w[p + 2], nw = w[p + 3];
    p += 4;
    if (kind < 0 || kind > 2 || nr < 0 || nw < 0 ||
        p + 2 * static_cast<size_t>(nr + nw) > w.size()) {
      std::fprintf(stderr, "bad record at word %zu\n", p - 4);
      return 2;
    }
    ++records[kind];
    auto it = index.find(tid);
    uint32_t t;
    if (it == index.end()) {
      t = static_cast<uint32_t>(txs.size());
      index.emplace(tid, t);
      txs.push_back({tid, kind});
    } else {
      t = it->second;
      if (txs[t].kind != kind) {
        std::fprintf(stderr, "tid %d logged with kinds %d and %d\n", tid,
                     txs[t].kind, kind);
        return 2;
      }
    }
    for (int32_t i = 0; i < nr; ++i, p += 2)
      reads.push_back({static_cast<uint32_t>(w[p]), static_cast<uint32_t>(w[p + 1]), t});
    for (int32_t i = 0; i < nw; ++i, p += 2)
      writes.push_back({static_cast<uint32_t>(w[p]), static_cast<uint32_t>(w[p + 1]), t});
  }
  const uint32_t n_tx = static_cast<uint32_t>(txs.size());
  uint64_t per_kind[3] = {0, 0, 0};
  int64_t mammoth = -1;
  for (uint32_t t = 0; t < n_tx; ++t) {
    ++per_kind[txs[t].kind];
    if (txs[t].kind == 2) {
      if (mammoth >= 0) std::printf("warning: more than one mammoth tid\n");
      mammoth = t;
    }
  }
  std::printf("records:\tepoch %lu, 2pl %lu, mammoth %lu\n",
              (unsigned long)records[0], (unsigned long)records[1],
              (unsigned long)records[2]);
  std::printf("committed_tx:\tepoch %lu, 2pl %lu, mammoth %lu\n",
              (unsigned long)per_kind[0], (unsigned long)per_kind[1],
              (unsigned long)per_kind[2]);
  std::printf("accesses:\treads %zu, writes %zu\n", reads.size(), writes.size());

  // Committed writes per node, ordered by version.
  auto by_node_version = [](const Access& a, const Access& b) {
    return a.node != b.node ? a.node < b.node : a.version < b.version;
  };
  std::sort(writes.begin(), writes.end(), by_node_version);
  uint64_t dup_versions = 0;
  for (size_t i = 1; i < writes.size(); ++i)
    if (writes[i].node == writes[i - 1].node &&
        writes[i].version == writes[i - 1].version)
      ++dup_versions;
  std::printf("duplicate_write_versions:\t%lu\n", (unsigned long)dup_versions);

  // Mammoth write version per node.
  std::unordered_map<uint32_t, uint32_t> mammoth_ver;
  if (mammoth >= 0) {
    mammoth_ver.reserve(writes.size() / 2);
    uint64_t twice = 0;
    for (const Access& a : writes)
      if (a.tx == static_cast<uint32_t>(mammoth))
        if (!mammoth_ver.emplace(a.node, a.version).second) ++twice;
    std::printf("mammoth_writes:\t%zu nodes (written twice: %lu)\n",
                mammoth_ver.size(), (unsigned long)twice);
  }

  // 1. Relation of short transactions to the mammoth, from their reads.
  if (mammoth >= 0) {
    std::vector<uint8_t> seen(n_tx, 0);  // bit 0: before, bit 1: after
    for (const Access& a : reads) {
      if (txs[a.tx].kind == 2) continue;
      auto it = mammoth_ver.find(a.node);
      if (it == mammoth_ver.end()) continue;
      seen[a.tx] |= (a.version >= it->second) ? 2 : 1;
    }
    for (int kind = 0; kind <= 1; ++kind) {
      uint64_t before = 0, after = 0, both = 0;
      for (uint32_t t = 0; t < n_tx; ++t) {
        if (txs[t].kind != kind) continue;
        if (seen[t] == 1) ++before;
        else if (seen[t] == 2) ++after;
        else if (seen[t] == 3) ++both;
      }
      std::printf("%s_vs_mammoth:\tbefore-only %lu, after-only %lu, "
                  "unsafe (read both) %lu\n",
                  kind == 0 ? "epoch" : "2pl", (unsigned long)before,
                  (unsigned long)after, (unsigned long)both);
    }
  }

  // 2. Dependency graph.
  std::vector<std::pair<uint32_t, uint32_t>> edges;
  edges.reserve(2 * reads.size() + writes.size());
  for (size_t i = 1; i < writes.size(); ++i)  // ww
    if (writes[i].node == writes[i - 1].node && writes[i].tx != writes[i - 1].tx)
      edges.push_back({writes[i - 1].tx, writes[i].tx});
  for (const Access& r : reads) {
    // Committed writes of this node: [lo, hi).
    Access key{r.node, 0, 0};
    auto lo = std::lower_bound(writes.begin(), writes.end(), key, by_node_version);
    key.version = UINT32_MAX;
    auto hi = std::upper_bound(writes.begin(), writes.end(), key, by_node_version);
    key.version = r.version;
    auto next = std::upper_bound(lo, hi, key, by_node_version);  // > version
    if (next != lo) {  // wr: the writer of the version read
      uint32_t writer = (next - 1)->tx;
      if (writer != r.tx) edges.push_back({writer, r.tx});
    }
    while (next != hi && next->tx == r.tx) ++next;  // its own later writes
    if (next != hi) edges.push_back({r.tx, next->tx});  // rw
  }
  std::printf("edges:\t%zu\n", edges.size());

  // CSR + iterative Tarjan.
  std::vector<uint32_t> start(n_tx + 1, 0), adj(edges.size());
  for (auto& e : edges) ++start[e.first + 1];
  for (uint32_t i = 0; i < n_tx; ++i) start[i + 1] += start[i];
  {
    std::vector<uint32_t> fill(start.begin(), start.end() - 1);
    for (auto& e : edges) adj[fill[e.first]++] = e.second;
  }
  edges.clear();
  edges.shrink_to_fit();
  const uint32_t kNone = UINT32_MAX;
  std::vector<uint32_t> idx(n_tx, kNone), low(n_tx), comp(n_tx, kNone);
  std::vector<uint8_t> on_stack(n_tx, 0);
  std::vector<uint32_t> stack, call, iter;
  uint32_t counter = 0, n_comp = 0;
  for (uint32_t s = 0; s < n_tx; ++s) {
    if (idx[s] != kNone) continue;
    call.push_back(s);
    iter.push_back(start[s]);
    idx[s] = low[s] = counter++;
    stack.push_back(s);
    on_stack[s] = 1;
    while (!call.empty()) {
      uint32_t v = call.back();
      uint32_t& it = iter.back();
      if (it < start[v + 1]) {
        uint32_t u = adj[it++];
        if (idx[u] == kNone) {
          idx[u] = low[u] = counter++;
          stack.push_back(u);
          on_stack[u] = 1;
          call.push_back(u);
          iter.push_back(start[u]);
        } else if (on_stack[u]) {
          low[v] = std::min(low[v], idx[u]);
        }
        continue;
      }
      if (low[v] == idx[v]) {
        uint32_t x;
        do {
          x = stack.back();
          stack.pop_back();
          on_stack[x] = 0;
          comp[x] = n_comp;
        } while (x != v);
        ++n_comp;
      }
      call.pop_back();
      iter.pop_back();
      if (!call.empty()) low[call.back()] = std::min(low[call.back()], low[v]);
    }
  }
  std::vector<uint32_t> size(n_comp, 0);
  for (uint32_t t = 0; t < n_tx; ++t) ++size[comp[t]];
  uint64_t cyc_comps = 0, cyc_tx = 0, cyc_kind[3] = {0, 0, 0};
  for (uint32_t t = 0; t < n_tx; ++t)
    if (size[comp[t]] > 1) {
      ++cyc_tx;
      ++cyc_kind[txs[t].kind];
    }
  for (uint32_t c = 0; c < n_comp; ++c)
    if (size[c] > 1) ++cyc_comps;
  std::printf("cycles:\t%lu strongly connected components with >1 tx, "
              "%lu tx in them (epoch %lu, 2pl %lu, mammoth %lu)\n",
              (unsigned long)cyc_comps, (unsigned long)cyc_tx,
              (unsigned long)cyc_kind[0], (unsigned long)cyc_kind[1],
              (unsigned long)cyc_kind[2]);
  if (mammoth >= 0) {
    uint32_t mc = comp[mammoth];
    std::printf("mammoth_component:\t%u tx\n", size[mc]);
    uint64_t other = 0;
    for (uint32_t c = 0; c < n_comp; ++c)
      if (c != mc && size[c] > 1) ++other;
    std::printf("cycles_without_mammoth:\t%lu components\n", (unsigned long)other);
  }
  std::printf("serializable:\t%s\n", cyc_comps == 0 ? "yes" : "NO");
  return 0;
}

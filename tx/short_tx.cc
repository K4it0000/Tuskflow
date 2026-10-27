#include "tx/short_tx.hh"

#include <algorithm>
#include <cstdio>

#include "util/thread_random.hh"

namespace tuskflow {

ShortTx::ShortTx(int64_t tid, GraphStorage* storage, ResultQueue* output,
                 const ShortTxConfig& cfg)
    : Tx(tid, storage, output), cfg_(cfg),
      // ZipfDistributionWithMappings(mappings, numberOfNodes - 1, 1)
      zipf_(cfg.number_of_nodes - 1, 1.0) {}

void ShortTx::run_locally() {
  protocol_->start_client(tid_, epoch_.load(std::memory_order_relaxed));
  logic(true);
}

// getNextId (LDBC: ranges[PERSON_TYPE].getLeft() + index)
uint32_t ShortTx::get_next_id() {
  if (cfg_.use_zipf)
    return cfg_.first_node +
           static_cast<uint32_t>((*cfg_.mappings)[zipf_(tl_rng())]);
  return cfg_.first_node +
         static_cast<uint32_t>(tl_rng().next() % cfg_.number_of_nodes);
}

// min(Zipf(size, 1).sample(), size - 1), or uniform.
uint32_t ShortTx::pick_index(const ZipfRejectionInversion* zipf_n, size_t size) {
  if (cfg_.use_zipf)
    return static_cast<uint32_t>(
        std::min<uint64_t>((*zipf_n)(tl_rng()), size - 1));
  return static_cast<uint32_t>(tl_rng().next() % size);
}

// singleReadOp: read a relationship and its end node.
uint32_t ShortTx::single_read_op(const ZipfRejectionInversion* zipf_n,
                                 const IntList& out) {
  uint32_t index = pick_index(zipf_n, out.size());
  uint32_t rel_id = static_cast<uint32_t>(out[index]);
  Relationship* rel = read_relationship(rel_id);
  uint32_t node_id = static_cast<uint32_t>(rel->end_node());
  read_node(node_id);
  return node_id;
}

// getNextNeighbour: one hop.
uint32_t ShortTx::get_next_neighbour(uint32_t node_id) {
  const IntList* out = read_out_neighbourhood(node_id);
  if (out == nullptr || out->empty()) return node_id;
  ZipfRejectionInversion zipf_n(out->size(), 1.0);
  return single_read_op(cfg_.use_zipf ? &zipf_n : nullptr, *out);
}

// --- RandomNodeTraversalReadsTx -------------------------------------------

bool ReadTx::logic(bool) {
  if (has_previous_work()) return run_previous_work();
  ser_fresh_attempt();
  uint32_t node_id = get_next_id();
  if (read_node(node_id) == nullptr) {
    std::printf("[DBG] node %u not found\n", node_id);
    return false;
  }
  for (int hop = 1; hop < cfg_.hops; ++hop) node_id = get_next_neighbour(node_id);
  return read_neighbours(node_id);
}

bool ReadTx::read_neighbours(uint32_t node_id) {
  const IntList* out = read_out_neighbourhood(node_id);
  if (out == nullptr || out->empty()) return true;
  ZipfRejectionInversion zipf_n(out->size(), 1.0);
  for (int i = 0; i < cfg_.ops - cfg_.hops; ++i)
    single_read_op(cfg_.use_zipf ? &zipf_n : nullptr, *out);
  return true;
}

// --- RandomNodeTraversalMixedTx -------------------------------------------

bool MixedTx::logic(bool local) {
  if (has_previous_work()) return run_previous_work();
  ser_fresh_attempt();
  uint32_t node_id = get_next_id();
  if (read_node(node_id) == nullptr) {
    std::printf("[DBG] node %u not found\n", node_id);
    return false;
  }
  for (int hop = 1; hop < cfg_.hops; ++hop) node_id = get_next_neighbour(node_id);
  return updates_neighbours_of_node(node_id, local);
}

bool MixedTx::updates_neighbours_of_node(uint32_t node_id, bool local) {
  const IntList* out = read_out_neighbourhood(node_id);
  if (out == nullptr || out->empty()) return true;
  ZipfRejectionInversion zipf_n(out->size(), 1.0);
  for (int i = 0; i < cfg_.ops - cfg_.hops; ++i) {
    uint32_t index = pick_index(cfg_.use_zipf ? &zipf_n : nullptr, out->size());
    uint32_t rel_id = static_cast<uint32_t>((*out)[index]);
    Relationship* rel = read_relationship(rel_id);
    if (rel == nullptr) {
      std::printf("[DBG] relationship %u not found\n", rel_id);
      return false;
    }
    update_node(static_cast<uint32_t>(rel->end_node()), local);
  }
  return true;
}

bool MixedTx::update_node(uint32_t node_id, bool local) {
  Node* node = prepare_update_node(node_id);
  if (node == nullptr) {
    std::printf("[DBG] node %u not found\n", node_id);
    return false;
  }
  write_new_property(node_id, node, local);
  return true;
}

// A local (epoch) write stages a copy; a 2PL write saves a copy of the old
// value and writes the stored node in place under the exclusive lock.
void ShortTx::write_new_property(uint32_t node_id, Node* node, bool local) {
  Node* copy = node->copy();
  copies_.push_back(copy);
  if (local) {
    copy->add_property(kNewPropertyKey, node_id);
    Protocol::update_last_write_set_entry(write_set_, nullptr, copy);
  } else {
    uint32_t v = node->add_property_in_place(kNewPropertyKey, node_id,
                                             storage_->version_ptr(node_id));
    if (SerLog::on()) ser_writes_.push_back({node_id, v});
    Protocol::update_last_write_set_entry(write_set_, copy, node);
  }
}

// --- LDBC: RandomFriendMessages{Read,Update}Tx -----------------------------

uint32_t FriendMessagesTx::get_next_ldbc_neighbour(uint32_t node_id) {
  const IntList* out = read_out_neighbourhood(node_id);
  if (out == nullptr || out->empty()) return node_id;  // end of the graph
  // serialScan
  IntList candidates;
  for (int32_t rel_id : *out) {
    Relationship* rel = storage_->get_relationship(static_cast<uint32_t>(rel_id));
    if (rel != nullptr && rel->type() == kPersonKnowsPerson)
      candidates.push_back(static_cast<int32_t>(rel->end_node()));
  }
  if (!candidates.empty()) {
    ZipfRejectionInversion zipf_n(candidates.size(), 1.0);
    uint32_t index =
        pick_index(cfg_.use_zipf ? &zipf_n : nullptr, candidates.size());
    return static_cast<uint32_t>(candidates[index]);
  }
  return node_id;
}

bool FriendMessagesReadTx::logic(bool) {
  if (has_previous_work()) return run_previous_work();
  ser_fresh_attempt();
  uint32_t node_id = get_next_id();
  if (read_node(node_id) == nullptr) {
    std::printf("[DBG] node %u not found\n", node_id);
    return false;
  }
  for (int hop = 1; hop < cfg_.hops; ++hop)
    node_id = get_next_ldbc_neighbour(node_id);
  return read_messages(node_id);
}

bool FriendMessagesReadTx::read_messages(uint32_t node_id) {
  Node* node = read_node(node_id);
  if (node == nullptr) {
    std::printf("[DBG] node %u not found\n", node_id);
    return false;
  }
  if (node->first_label() != cfg_.ldbc->person_label) {
    std::printf("[DBG] node %u was not a message\n", node_id);
    return false;
  }
  const IntList* in = read_in_neighbourhood(node_id);
  if (in == nullptr || in->empty()) return true;  // end of the graph
  ZipfRejectionInversion zipf_n(in->size(), 1.0);
  for (int i = 0; i < cfg_.ops - cfg_.hops; ++i) {
    uint32_t index = pick_index(cfg_.use_zipf ? &zipf_n : nullptr, in->size());
    uint32_t rel_id = static_cast<uint32_t>((*in)[index]);
    Relationship* rel = read_relationship(rel_id);
    if (rel == nullptr) {
      std::printf("[DBG] relationship %u not found\n", rel_id);
      return false;
    }
    if (is_has_creator(*rel)) {
      uint32_t message_id = static_cast<uint32_t>(rel->start_node());
      Node* message = read_node(message_id);
      if (message == nullptr) {
        std::printf("[DBG] Message %u not found\n", message_id);
        return false;
      }
      if (!is_message_label(message->first_label())) {
        std::printf("[DBG] node %u was not a message\n", message_id);
        return false;
      }
    }
  }
  return true;
}

bool FriendMessagesUpdateTx::logic(bool local) {
  if (has_previous_work()) return run_previous_work();
  ser_fresh_attempt();
  uint32_t node_id = get_next_id();
  if (read_node(node_id) == nullptr) {
    std::printf("[DBG] node %u not found\n", node_id);
    return false;
  }
  for (int hop = 1; hop < cfg_.hops; ++hop)
    node_id = get_next_ldbc_neighbour(node_id);
  return updates_messages_of_node(node_id, local);
}

bool FriendMessagesUpdateTx::updates_messages_of_node(uint32_t node_id,
                                                      bool local) {
  const IntList* in = read_in_neighbourhood(node_id);
  if (in == nullptr || in->empty()) return true;  // end of the graph
  ZipfRejectionInversion zipf_n(in->size(), 1.0);
  for (int i = 0; i < cfg_.ops - cfg_.hops; ++i) {
    uint32_t index = pick_index(cfg_.use_zipf ? &zipf_n : nullptr, in->size());
    uint32_t rel_id = static_cast<uint32_t>((*in)[index]);
    Relationship* rel = read_relationship(rel_id);
    if (rel == nullptr) {
      std::printf("[DBG] relationship %u not found\n", rel_id);
      return false;
    }
    if (is_has_creator(*rel) &&
        !update_message(static_cast<uint32_t>(rel->start_node()), local))
      return false;
  }
  return true;
}

bool FriendMessagesUpdateTx::update_message(uint32_t node_id, bool local) {
  Node* node = prepare_update_node(node_id);
  if (node == nullptr) {
    std::printf("[DBG] node %u not found\n", node_id);
    return false;
  }
  if (!is_message_label(node->first_label())) {
    std::printf("[DBG] node %u was not a message\n", node_id);
    return false;
  }
  write_new_property(node_id, node, local);
  return true;
}

}  // namespace tuskflow

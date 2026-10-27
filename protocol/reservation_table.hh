#pragma once

// Reservation table of the original TuskFlow (runtime/dependencies/):
// ReservationEntry, LongRunningState, ReservationState, ReservationTableImpl.
//
// Paper Algorithm 1 (TuskFlow, PVLDB 18(12)):
//   l.14 mergeRead          -> ReservationTable::merge_read
//   l.16 mergeWrite         -> ReservationTable::merge_write
//   l.18 mergeMammothRead   -> ReservationTable::merge_mammoth_read
//   l.20 mergeMammothWrite  -> ReservationTable::merge_mammoth_write
//   l.30-36 (one entry of HasConflicts) -> ReservationTable::check_entry
// The loop of HasConflicts (l.26-37) is ReservationProtocol::has_conflicts.

#include <algorithm>
#include <climits>
#include <cstdint>

#include "util/chm.hh"

namespace tuskflow {

enum class ResourceType : uint8_t { NODE = 0, RELATIONSHIP = 1, NODE_RELATIONSHIPS = 2 };
constexpr int kResourceTypes = 3;

enum class LongRunningState : uint8_t {
  UNVISITED = 0,
  PENDING_READ = 1,
  PENDING_WRITE = 2,
  VISITED = 3,
};

inline LongRunningState transition_to_state(LongRunningState from,
                                            LongRunningState to) {
  return (static_cast<int>(from) <= static_cast<int>(to)) ? to : from;
}

struct ReservationEntry {
  int epoch;
  int readTid;
  int writeTid;
  LongRunningState state;
  bool writtenByLongRunning;

  // ReservationEntry.merge: updates this entry in place.  Within an epoch
  // the smallest TID wins the read and the write reservation; an entry of an
  // older epoch is replaced.  The mammoth state only moves forward.
  void merge(const ReservationEntry& entry) {
    if (entry.epoch == epoch) {
      readTid = std::min(readTid, entry.readTid);
      writeTid = std::min(writeTid, entry.writeTid);
      state = transition_to_state(state, entry.state);
      writtenByLongRunning |= entry.writtenByLongRunning;
    } else if (entry.epoch > epoch) {
      if (entry.writeTid != INT_MAX) {
        epoch = entry.epoch;
        writeTid = entry.writeTid;
        readTid = INT_MAX;
      } else if (entry.readTid != INT_MAX) {
        epoch = entry.epoch;
        readTid = entry.readTid;
        writeTid = INT_MAX;
      }
      state = transition_to_state(state, entry.state);
      writtenByLongRunning |= entry.writtenByLongRunning;
    }
  }
};

inline constexpr ReservationEntry kDefaultEntry{-1, INT_MAX, INT_MAX,
                                                LongRunningState::UNVISITED,
                                                false};

struct ReservationState {
  bool seenUnvisited = false;
  bool seenVisited = false;
  bool hasRAW = false;
  bool hasWAR = false;
  void reset() { *this = ReservationState{}; }
};

class ReservationTable {
public:
  // `new ConcurrentHashMap<>(16, 0.6f, 512)` per resource type.
  ReservationTable()
      : maps_{Map(16, 0.6, 512), Map(16, 0.6, 512), Map(16, 0.6, 512)} {}

  // Alg.1 l.14 mergeRead (Java tryToReserveRead).  True if this transaction
  // holds the read reservation (smallest TID).
  bool merge_read(ResourceType type, int64_t id, int tid, int epoch) {
    ReservationEntry* e = reserve(
        type, id,
        {epoch, tid, INT_MAX, LongRunningState::UNVISITED, false});
    return e->readTid == tid;
  }
  // Alg.1 l.16 mergeWrite (Java tryToReserveWrite).
  bool merge_write(ResourceType type, int64_t id, int tid, int epoch) {
    ReservationEntry* e = reserve(
        type, id,
        {epoch, INT_MAX, tid, LongRunningState::UNVISITED, false});
    return e->writeTid == tid;
  }
  // Alg.1 l.18 mergeMammothRead (Java reserveLongRunningRead): sets the
  // mammoth state, no TID.
  void merge_mammoth_read(ResourceType type, int64_t id,
                          LongRunningState state, int epoch) {
    reserve(type, id, {epoch, INT_MAX, INT_MAX, state, false});
  }
  // Alg.1 l.20 mergeMammothWrite (Java reserveLongRunningWrite): sets the
  // mammoth state and hasMammothWrite, no TID.
  void merge_mammoth_write(ResourceType type, int64_t id,
                           LongRunningState state, int epoch) {
    reserve(type, id, {epoch, INT_MAX, INT_MAX, state, true});
  }

  // Alg.1 l.30-36 for one read or write of a transaction (Java
  // ReservationTableImpl.hasConflict).  `state` carries seenVisited /
  // seenUnvisited (and RAW / WAR) across the entries of the transaction.
  // Differences from the pseudo-code, as in the original:
  //   - reordering (paper Sec. 4, on by default): a RAW or a WAR alone does
  //     not abort, only WAW or RAW together with WAR (l.31 aborts on WAW and
  //     RAW);
  //   - tag optimization (paper Sec. 4): the mammoth-state check (l.33-36)
  //     only looks at the resource types the mammoth updates.
  bool check_entry(ResourceType type, int64_t id, int64_t tid, int epoch,
                   ReservationState& state, bool is_read) const {
    ReservationEntry* found = nullptr;
    const ReservationEntry& entry =
        maps_[static_cast<int>(type)].get(id, found) ? *found : kDefaultEntry;
    bool conflict = false;
    if (reordering_) {
      state.hasRAW |= is_read && (epoch == entry.epoch) && (tid > entry.writeTid);
      state.hasWAR |= !is_read && (epoch == entry.epoch) && (tid > entry.readTid);
      bool waw = !is_read && (epoch == entry.epoch) && (tid > entry.writeTid);
      conflict |= (state.hasRAW && state.hasWAR) || waw;
    } else {
      conflict |= (epoch == entry.epoch) && (tid > entry.writeTid);
    }
    // l.33-36.  Label indexes (set only for the LDBC mammoth): as in the
    // original, the check uses the id alone, whatever the resource type.
    bool label_found = !has_label_range_ ||
                       (label_first_ <= id && id <= label_last_);
    if (label_found &&
        (resources_updated_ == 0 ||
         (resources_updated_ & (1u << static_cast<int>(type))) != 0)) {
      bool is_visited = entry.state == LongRunningState::VISITED &&
                        entry.writtenByLongRunning;
      bool is_pending = entry.state == LongRunningState::PENDING_WRITE ||
                        (entry.state == LongRunningState::PENDING_READ && !is_read);
      bool is_unvisited = entry.state == LongRunningState::UNVISITED;
      state.seenVisited |= is_visited;
      state.seenUnvisited |= is_unvisited;
      conflict |= (state.seenUnvisited && state.seenVisited) || is_pending;
    }
    return conflict;
  }

  // ReservationTableImpl.clear (also drops the tag set).  The original runs
  // it after the mammoth, when no transaction uses the table, so entries are
  // freed directly.
  void clear() {
    for (auto& m : maps_) {
      m.for_each([](int64_t, ReservationEntry* e) { delete e; });
      m.clear_exclusive();
    }
    resources_updated_ = 0;  // the label indexes stay, as in the original
  }

  // setResourcesUpdated (USE_TAG_OPTIMIZATION).
  void add_resource_updated(ResourceType type) {
    resources_updated_ |= 1u << static_cast<int>(type);
  }
  // setLabelsUpdated (USE_TAG_OPTIMIZATION) with one index, the ids
  // [first, last] (LdbcMetadata.messageIds).
  void set_label_range(int64_t first, int64_t last) {
    label_first_ = first;
    label_last_ = last;
    has_label_range_ = true;
  }
  void enable_reordering() { reordering_ = true; }

  int64_t entries(ResourceType type) const {
    return maps_[static_cast<int>(type)].size();
  }
  size_t bins(ResourceType type) const {
    return maps_[static_cast<int>(type)].bins();
  }
  uint64_t resizes() const {
    uint64_t r = 0;
    for (const auto& m : maps_) r += m.resizes();
    return r;
  }
  uint64_t resize_ns() const {
    uint64_t r = 0;
    for (const auto& m : maps_) r += m.resize_ns();
    return r;
  }
  size_t approx_bytes() const {
    size_t b = 0;
    for (const auto& m : maps_)
      b += m.approx_bytes() +
           static_cast<size_t>(m.size()) * sizeof(ReservationEntry);
    return b;
  }

private:
  using Map = ConcurrentHashMap<int64_t, ReservationEntry*>;
  Map maps_[kResourceTypes];
  uint32_t resources_updated_ = 0;
  bool has_label_range_ = false;
  int64_t label_first_ = 0, label_last_ = -1;
  bool reordering_ = false;

  // map.merge(id, new ReservationEntry(...), (prev, curr) -> prev.merge(curr)).
  // The original allocates `curr` on every call and discards it when the key
  // exists; only the entry that gets stored is allocated here.
  ReservationEntry* reserve(ResourceType type, int64_t id,
                            const ReservationEntry& curr) {
    ReservationEntry* made = nullptr;
    ReservationEntry* e = maps_[static_cast<int>(type)].merge(
        id, [&] { return made = new ReservationEntry(curr); },
        [&](ReservationEntry* prev) {
          prev->merge(curr);  // under the bin lock, as in the JDK
          return prev;
        });
    if (made != nullptr && made != e) delete made;  // lost an insertion race
    return e;
  }
};

}  // namespace tuskflow

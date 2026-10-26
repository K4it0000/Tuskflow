#pragma once

// Run-time configuration (DBConfig of the original).  Defined in config.cc.

#include <gflags/gflags.h>

DECLARE_uint64(nodes);
DECLARE_uint64(rels);
DECLARE_uint64(seed);
DECLARE_uint64(node_capacity);
DECLARE_uint64(rel_capacity);
DECLARE_uint64(workers);
DECLARE_uint64(clients);
DECLARE_uint64(client_tx_per_sec);
DECLARE_uint64(read_ratio);
DECLARE_uint64(ops);
DECLARE_uint64(hops);
DECLARE_bool(use_zipf);
DECLARE_uint64(epoch_size);
DECLARE_bool(start_lr);
DECLARE_uint64(lr_budget);
DECLARE_uint64(lr_delay_ms);
DECLARE_uint64(duration_ms);
DECLARE_bool(reordering);
DECLARE_bool(tag_optimization);
DECLARE_bool(track_metadata);
DECLARE_uint64(tracking_threshold);
DECLARE_string(epoch_log);
DECLARE_bool(fix_getnext_race);
DECLARE_bool(lr_breakdown);
DECLARE_string(ser_log);
DECLARE_bool(fix_mode_switch_race);

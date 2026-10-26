#pragma once

// Counters that are not in the original: aborts, the scheduler-race and 2PL
// diagnostics and the mammoth cost breakdown.  Defined in counters.cc.

#include <atomic>

#include "util/padded_counter.hh"

namespace tuskflow {

extern PaddedCounter g_aborts;
extern PaddedCounter g_diag_overflow_resubmit;
extern PaddedCounter g_diag_stale_poll;
extern PaddedCounter g_diag_commit_outside_p2;
extern PaddedCounter g_diag_local_outside_p1;
extern PaddedCounter g_diag_put_back;
extern PaddedCounter g_commits_2pl;
extern PaddedCounter g_2pl_deadlock_retries;
extern PaddedCounter g_2pl_failed;
extern PaddedCounter g_inflight_2pl;
extern PaddedCounter g_diag_2pl_converted;
extern PaddedCounter g_diag_2pl_in_epoch;
extern PaddedCounter g_diag_2pl_overlap;
extern PaddedCounter g_diag_inflight_at_switch;
extern std::atomic<int>* g_execution_mode;
extern PaddedCounter g_lr_phase1_ns;
extern PaddedCounter g_lr_phase2_ns;
extern PaddedCounter g_lr_cyc_reserve;
extern PaddedCounter g_lr_cyc_get;
extern PaddedCounter g_lr_cyc_copy;
extern PaddedCounter g_lr_cyc_rest;
extern PaddedCounter g_lr_cyc_merge;
extern PaddedCounter g_lr_cyc_free;
extern double g_tsc_ghz;  // TSC ticks per ns, calibrated at start

}  // namespace tuskflow

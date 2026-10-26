#include "monitor/counters.hh"

namespace tuskflow {

PaddedCounter g_aborts;          // not counted by the original
// Diagnostics for the getNext() race; they only
// count, the behaviour is unchanged.
PaddedCounter g_diag_overflow_resubmit;  // workCounter < 0 -> task re-submitted
PaddedCounter g_diag_stale_poll;         // epoch or mode changed between the
                                         // mode read and the poll
PaddedCounter g_diag_commit_outside_p2;  // COMMIT task run while mode != 2
PaddedCounter g_diag_local_outside_p1;   // EXECUTE_LOCALLY run while mode != 1
PaddedCounter g_diag_put_back;           // --fix_getnext_race: stale poll undone
// 2PL / mode switch; not in the original.
PaddedCounter g_commits_2pl;             // committed in 2PL mode
PaddedCounter g_2pl_deadlock_retries;    // DeadlockDetectedException, retried
PaddedCounter g_2pl_failed;              // gave up after TX_RETRIES
PaddedCounter g_inflight_2pl;            // 2PL transactions running now
PaddedCounter g_diag_2pl_converted;      // 2PL task moved to pending in mode 1
PaddedCounter g_diag_2pl_in_epoch;       // ... while it used an epoch slot
PaddedCounter g_diag_2pl_overlap;        // 2PL tx ran while mode != 0
PaddedCounter g_diag_inflight_at_switch; // 2PL txs running at the first epoch
std::atomic<int>* g_execution_mode = nullptr;
PaddedCounter g_lr_phase1_ns;    // mammoth cost breakdown (not in the original)
PaddedCounter g_lr_phase2_ns;
// --lr_breakdown: TSC cycles per mammoth step, summed over the run.
PaddedCounter g_lr_cyc_reserve, g_lr_cyc_get, g_lr_cyc_copy, g_lr_cyc_rest;
PaddedCounter g_lr_cyc_merge, g_lr_cyc_free;
double g_tsc_ghz = 1.0;

}  // namespace tuskflow

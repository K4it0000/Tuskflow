// Run-time configuration: the fields of the original's DBConfig, as gflags
// with the original defaults (except --tag_optimization, see the help text).

#include "config.hh"

DEFINE_string(dataset, "random", "DATASET: random or ldbc.");
DEFINE_string(ldbc_path, "",
              "LDBC SNB datagen raw CSV directory (.../graphs/csv/raw/"
              "composite-merged-fk), for --dataset=ldbc.  The LDBC workload is "
              "USE_TRAVERSALS = true with the mammoth LDBC_QUERY_TYPE = 0.");
DEFINE_uint64(nodes, 30000000, "RandomLoader NODES_SIZE.");
DEFINE_uint64(rels, 100000000, "RandomLoader RELS_SIZE.");
DEFINE_uint64(seed, 42, "Seed of the random graph (the original is unseeded; "
              "same generator as GraphStorage::build_tuskflow_random).");
DEFINE_uint64(node_capacity, 36000000,
              "DBConfig.INITIAL_NODES_SIZE when the storage is created (array "
              "capacity, rounded up to a power of two).");
DEFINE_uint64(rel_capacity, 160000000,
              "DBConfig.INITIAL_RELS_SIZE when the storage is created.");
DEFINE_uint64(workers, 0, "Worker threads; 0 = hardware threads "
              "(Runtime.availableProcessors()).");
DEFINE_uint64(clients, 100, "NUMBER_OF_CLIENTS.");
DEFINE_uint64(client_tx_per_sec, 100, "CLIENT_TX_PER_SEC (0 = unthrottled).");
DEFINE_uint64(read_ratio, 100, "READ_RATIO (percent read-only transactions).");
DEFINE_uint64(ops, 10, "NUMBER_OF_OPS.");
DEFINE_uint64(hops, 1, "NUMBER_OF_HOPS.");
DEFINE_bool(use_zipf, true, "USE_ZIPF_DISTRIBUTION (shuffled mapping).");
DEFINE_uint64(epoch_size, 10000, "EPOCH_SIZE.");
DEFINE_bool(start_lr, true, "START_LONG_RUNNING_TX.");
DEFINE_uint64(lr_budget, 500000, "LONG_RUNNING_EXECUTION_BUDGET.");
DEFINE_uint64(lr_delay_ms, 100000, "LONG_RUNNING_TX_DELAY_IN_MILLIS.");
DEFINE_uint64(duration_ms, 150000, "EXPERIMENT_DURATION_IN_MILLIS.");
DEFINE_bool(reordering, true, "USE_REORDERING_OPTIMIZATION.");
DEFINE_bool(tag_optimization, false,
            "USE_TAG_OPTIMIZATION (original default true; false is the "
            "minimal configuration).");
DEFINE_bool(track_metadata, true,
            "GraphStorage access tracking for the partitioners (on in the "
            "original: createGraphStorage(true)).");
DEFINE_uint64(tracking_threshold, 40, "TRACKING_THRESHOLD (percent).");
DEFINE_string(epoch_log, "", "If set, write one line per non-empty epoch.");
DEFINE_bool(fix_getnext_race, false,
            "Fix the original's getNext() race: "
            "after polling, check that the epoch and the execution mode are "
            "unchanged and otherwise put the task back.  false = original "
            "behaviour (a worker that read a stale mode can take a task of the "
            "next epoch, re-submit it to pending or commit it during Phase 1).");
DEFINE_bool(lr_breakdown, false,
            "Measure the mammoth's per-step cost with rdtsc (Phase 1: reserve, "
            "getNode, copy, addProperty+write set; Phase 2: merge, free).  Not "
            "in the original; adds a few ns per step.");
DEFINE_string(ser_log, "",
              "If set, write a serializability log to this file: "
              "for every committed transaction the (node, version) pairs it "
              "read and wrote.  Not in the original; for correctness runs.");
DEFINE_bool(fix_mode_switch_race, false,
            "Fix the original's races at the switch from 2PL to epochs: "
            "wait for running 2PL transactions "
            "to finish before the first epoch, and give back the epoch slot "
            "of a 2PL task found during Phase 1.  false = original behaviour.");

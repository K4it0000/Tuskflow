# TuskFlow の C++ 実装

TuskFlow 論文 (PVLDB 18(12)) の Java プロトタイプ (以下「原典」) を、クラス単位で C++ に移したもの。原典の動作を再現することを目的にしている。

## 実装内容

| 対象 | 内容 |
|---|---|
| ワークロード | 原典の乱数グラフと、LDBC SNB (datagen の raw 形式 CSV を読む) |
| 短 tx | 乱数グラフ: `RandomNodeTraversal{Reads,Mixed}Tx`。LDBC: `RandomFriendMessages{Read,Update}Tx` (Person の in リストから辺を選び、hasCreator ならメッセージを読むか書く) |
| mammoth | 乱数グラフ: `AllNodesLongRunningTx` (全頂点に書く)。LDBC: Q0 `AllMessagesLongRunningTx` (全メッセージに書く) |
| epoch 方式 | 論文 Algorithm 1 (Phase 1 で予約、Phase 2 で判定と書き戻し)、reordering、tag 最適化、abort 後の再生 |
| 2PL | mammoth が動いていない間の実行と、epoch 方式との切り替え。Neo4j 5.16.0 の Forseti ロックマネージャ |
| クライアント | open-loop。100 クライアントが毎秒 100 tx ずつ投入する |
| 計測 | スループット、レイテンシの分布、mammoth の長さ、Phase ごとの時間、abort 数、epoch ごとの記録 (`--epoch_log`) |
| 正しさの検査 | `--ser_log` で記録し、`tuskflow_ser_check.exe` で衝突直列化可能かを調べる |

**未実装**: LDBC の Q1・Q2、メッセージだけの短 tx (`USE_TRAVERSALS = false`)、mammoth の分割・並列実行、2PL での mammoth の実行。

**原典との主な違い**: GC の代わりに明示的に解放する (他のスレッドが読みうるものは QSBR で回収する)。メモリ確保には mimalloc を使う。Java ではデータ競合になるノードのマージを、ノードごとのロックで守る。そのほかの違いは各ファイルのコメントに書いてある。原典の競合 2 つは比較のため既定で残しており、`--fix_getnext_race` と `--fix_mode_switch_race` で直せる。

## 動かし方

**必要なもの**: x86_64 の Linux、GCC 11 以降、CMake 3.14 以降、gflags (`apt install build-essential cmake git libgflags-dev`)。mimalloc は configure 時に取得するので、ネットワーク接続が要る。

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
TF=build  # 実行ファイルの場所

# 乱数グラフ (動作確認)
$TF/tuskflow.exe --nodes=100000 --rels=330000 --duration_ms=6000 --lr_delay_ms=2000

# LDBC SNB (論文 §6.2 の条件: 1-hop、開始 Person は一様、読み取り 80%)
$TF/tuskflow.exe --dataset=ldbc \
  --ldbc_path=<datasets>/ldbc/sf3-raw/graphs/csv/raw/composite-merged-fk \
  --hops=1 --use_zipf=false --read_ratio=80 --duration_ms=90000 --lr_delay_ms=60000

# 正しさの検査
$TF/tuskflow.exe --nodes=100000 --rels=330000 --duration_ms=6000 --lr_delay_ms=2000 --ser_log=ser.bin
$TF/tuskflow_ser_check.exe ser.bin
```

| 規模 | 頂点 / 辺 | 最大メモリ |
|---|---|---|
| 乱数グラフ (動作確認) | 10 万 / 33 万 | 約 5 GB |
| 乱数グラフ (1,000 万頂点) | 1,000 万 / 3,300 万 | 約 11 GB |
| LDBC SF3 | 1,064 万 / 4,910 万 | 約 12 GB |
| LDBC SF10 | 3,390 万 / 1 億 6,556 万 | 未計測 |

- フラグを何も付けないと原典の既定値 (3,000 万頂点、150 秒) で動くので、規模は必ず指定する。全フラグと既定値は [config.cc](config.cc) にある。
- 正しさの検査は、既定の設定では原典の競合のため `NO` になることがある。上の 2 つのフラグを付けると `yes` になる。tag 最適化を有効にすると、原典と同じく `NO` になる。
- 「mammoth 中のスループット」は、最初の数 epoch の速さに左右される指標なので、比べるときは epoch ごとの commit 数も見る。

## 読む順番

1. **[tx/transaction.hh](tx/transaction.hh)**: トランザクション 1 本が Algorithm 1 のどの段階を通るか (`execute_tx`)。
2. **[protocol/reservation_protocol.hh](protocol/reservation_protocol.hh)** と **[protocol/reservation_table.hh](protocol/reservation_table.hh)**: 予約と、commit の判定 (HasConflicts)。
3. **[tx/short_tx.cc](tx/short_tx.cc)** と **[tx/mammoth_tx.cc](tx/mammoth_tx.cc)**: 短 tx と mammoth の中身。
4. **[runtime/query_manager.cc](runtime/query_manager.cc)**: epoch の進め方 (Phase 1 / Phase 2 と barrier) と、2PL との切り替え。

## 論文 Algorithm 1 との対応

| Alg. 1 | 内容 | 関数 |
|---|---|---|
| l.1–9 `ExecuteTx` | 実行段階なら RunLocally して commit 段階へ、commit 段階なら TryToRunOnDatabase し、失敗か mammoth の続きがあれば次の epoch へ | `Tx::execute_tx` ([tx/transaction.cc](tx/transaction.cc)) |
| l.10–12 `RunLocally` | 最新の状態を読み、ローカルに実行して予約する (mammoth は budget まで) | `ShortTx::run_locally`、`AllNodesLongRunningTx::run_locally` |
| l.13–14 `ReserveRead` | 読みの予約 | `ReservationProtocol::reserve_read` → `ReservationTable::merge_read` |
| l.15–16 `ReserveWrite` | 書きの予約 | `ReservationProtocol::reserve_write` → `ReservationTable::merge_write` |
| l.17–18 `ReserveMammothRead` | mammoth の読み (状態だけ) | `ReservationTable::merge_mammoth_read` (この mammoth は使わない) |
| l.19–20 `ReserveMammothWrite` | mammoth の書き (状態と hasMammothWrite) | `ReservationProtocol::reserve_mammoth_write` → `ReservationTable::merge_mammoth_write` |
| l.21–25 `TryToRunOnDatabase` | 競合がなければ (mammoth は無条件に) 書き込みを反映する | `Tx::try_to_run_on_database`、`AllNodesLongRunningTx::try_to_run_on_database` |
| l.26–37 `HasConflicts` | 読み・書きの各エントリについて WAW / RAW と、mammoth の状態 (straddle、pending) を調べる | `ReservationProtocol::has_conflicts` (ループ) → `ReservationTable::check_entry` (1 エントリ) |

原典 (= この実装) は Algorithm 1 より次の点が多い。いずれもコメントに書いてある。

- **reordering** (論文 §4、既定で有効): RAW と WAR の片方だけなら abort しない。`check_entry`。
- **tag 最適化** (論文 §4、`--tag_optimization`): straddle の判定を、mammoth が書く資源の種類に絞る。`check_entry`。
- **abort 後の再生**: abort した短 tx は、ロジックを実行し直さず、前回の読み書きを予約し直す。`Tx::run_previous_work`。
- **2PL**: mammoth が動いていない間は、Neo4j の Forseti ロックで 1 段階で実行する。`Tx::run_on_database_with_retries`、[protocol/two_pl_protocol.hh](protocol/two_pl_protocol.hh)、[protocol/forseti.hh](protocol/forseti.hh)。

## ファイルと原典のクラス

| ファイル | 原典 (`org.neo4j.longrunning.` 以下) |
|---|---|
| [main.cc](main.cc) | `benchmarks.PrototypeBenchmark` (起動、実験、結果の出力)、`Database` の起動部分 |
| [config.hh](config.hh) / [config.cc](config.cc) | `configuration.DBConfig` (フラグと既定値) |
| [storage/entities.hh](storage/entities.hh) | `database.storage.entities.*` (`Node`、`PropertyMap`、`Property`、`LabelMap`、`Relationship`、`AtomicObjectArray`) |
| [storage/graph_storage.hh](storage/graph_storage.hh) | `database.storage.GraphStorage`、`MetadataStore` |
| [storage/random_loader.cc](storage/random_loader.cc) | `benchmarks.data.RandomLoader`、`BenchmarkUtils.populateDatabase` / `initializeMappings` |
| [storage/ldbc_loader.cc](storage/ldbc_loader.cc) | `benchmarks.data.LdbcLoader`、`benchmarks.data.metadata.LdbcMetadata`、`BenchmarkUtils.populateDatabase` / `initializeMappings` の LDBC 部分 |
| [protocol/reservation_table.hh](protocol/reservation_table.hh) | `runtime.dependencies.ReservationTableImpl`、`ReservationEntry`、`LongRunningState`、`ReservationState` |
| [protocol/protocol.hh](protocol/protocol.hh) | `tasks.protocol.ConcurrencyProtocol`、`utils.RWKey` |
| [protocol/reservation_protocol.hh](protocol/reservation_protocol.hh) | `tasks.protocol.NoLocksWithReservationImpl` |
| [protocol/two_pl_protocol.hh](protocol/two_pl_protocol.hh) | `tasks.protocol.TwoPLImpl` |
| [protocol/forseti.hh](protocol/forseti.hh) | Neo4j 5.16.0 の `ForsetiLockManager` / `ForsetiClient` / `SharedLock` / `ExclusiveLock` (jar のバイトコードから移植) |
| [tx/task.hh](tx/task.hh) | `tasks.Task`、`TaskPool`、`TransactionType`、`database.Result` |
| [tx/transaction.hh](tx/transaction.hh) | `tasks.AbstractTransaction` |
| [tx/short_tx.hh](tx/short_tx.hh) | `tasks.storedprocedures.RandomNodeTraversalReadsTx` / `RandomNodeTraversalMixedTx`、`tasks.storedprocedures.ldbc.RandomFriendMessagesReadTx` / `RandomFriendMessagesUpdateTx` |
| [tx/mammoth_tx.hh](tx/mammoth_tx.hh) | `tasks.storedprocedures.AllNodesLongRunningTx`、`tasks.storedprocedures.ldbc.AllMessagesLongRunningTx`、`tasks.AbstractLongRunningTransaction` |
| [runtime/query_manager.hh](runtime/query_manager.hh) | `runtime.LongRunningQueryManager` |
| [runtime/worker.cc](runtime/worker.cc) | `runtime.Worker` |
| [runtime/database.hh](runtime/database.hh) | `database.Database`、`database.CypherParser.parseRandom`、`benchmarks.workers.applicationclients.PrototypeClient` / `PrototypeLongRunningClient` |
| [monitor/metrics.hh](monitor/metrics.hh) / [monitor/report.hh](monitor/report.hh) | `monitoring.Metrics`、`MetricsMonitor` |
| [monitor/counters.hh](monitor/counters.hh) | (原典にない) abort 数、競合の診断、mammoth の内訳 |
| [monitor/ser_log.hh](monitor/ser_log.hh) | (原典にない) 正しさの検証用の記録 `--ser_log` |
| [util/](util/) | JDK の代わり: `ConcurrentHashMap` (`chm.hh`)、GC の代わり (`qsbr.hh`)、`ConcurrentLinkedQueue` / `CountDownLatch` / `java.util.Random` (`concurrency.hh`)、`ThreadLocalRandom` (`thread_random.hh`、`random.hh`)、commons-math の `ZipfDistribution` (`zipf.hh`)、メモリ確保 (`mimalloc_new.cc`) |
| [tools/](tools/) | 正しさの検査 (`ser_check.cc`、`--ser_log` の記録を読む) |

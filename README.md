# PostgreSQL Concurrency Tester

PostgreSQL Concurrency Tester runs controlled PostgreSQL concurrency experiments and reports client-observed request latency, connection-pool waiting, SQL round trips, throughput, and correctness. A query can be quick alone but become a bottleneck when transactions compete for the same row.

## Current capabilities

- A Docker PostgreSQL 18 lab with `pg_stat_statements` preloaded.
- A deterministic, resettable inventory-hotspot workload using a shared row.
- A `pool-starvation` variant that keeps application connections checked out after commit, so pool waiting can be observed separately from the transaction's brief row-lock time.
- An `index-contention` read workload over 50,000 deterministic order rows, with before/after indexed variants and captured `EXPLAIN (FORMAT JSON)` plans.
- Closed-loop workers with bounded concurrency and configurable pool size.
- HDR histogram percentiles from recorded request samples, pool-acquisition timing, SQL round trips, and throughput.
- A separate bounded monitor pool sampling workload sessions, wait events, transaction age, lock metadata, and `pg_blocking_pids()` relationships.
- Database-scoped counter deltas for commits, rollbacks, deadlocks, and block activity, invalidated when the PostgreSQL stats reset marker changes or counters decrease.
- `pg_stat_statements` before/after deltas grouped by PostgreSQL database/user/query ID/top-level status, with query text omitted and reset/eviction invalidation.
- JSON and Markdown reports with an inventory invariant check and evidence-based findings.
- A local HTTP endpoint serving report history.
- A JSON baseline/candidate comparison command with workload-compatibility checks, correctness rejection, and optional p95/error-rate regression gates.
- A sequential, bounded experiment matrix over concurrency and pool sizes.

The lab separates its bootstrap administrator, workload role, and monitor role. The monitor role receives PostgreSQL's read-only `pg_monitor` privileges. `MONITOR_DATABASE_URL` selects its connection; `DATABASE_URL` is used only for workload sessions. The current runner supports closed-loop workloads and three scenarios: inventory hotspot, pool starvation, and indexed reads. Open-loop scheduling, deadlock reproduction, reservation contention, comparison across concurrency sweeps, and the planned React dashboard are not implemented yet.

## Quick start

Requirements: Rust 1.85 or newer, Cargo, and Docker Compose.

```sh
docker compose -f lab/compose.yaml up -d
cargo build --release
cargo run -- doctor
cargo run -- lab init --seed 42
cargo run -- demo --concurrency 16 --seconds 10 --pool-size 8 --hold-ms 5
cargo run -- demo --scenario pool-starvation --concurrency 32 --pool-size 4 --hold-ms 20
cargo run -- matrix --scenario pool-starvation --concurrency 16,32 --pool-sizes 2,4,8 --seconds 5
cargo run -- serve
```

The demo executes workloads against the real lab. In `inventory-hotspot`, each transaction updates a shared inventory row, holds the transaction open for the configured interval, restores the quantity, and commits. In `pool-starvation`, the transaction is committed first and the acquired client connection is held for the interval. In `index-contention`, concurrent SELECT statements run against a seeded 50,000-row table; pass `--indexed` to create the composite index before that run. The report captures planner JSON without running `EXPLAIN ANALYZE`. Run artifacts are written to `runs/`. The initial seed/setup is explicit; `lab init` resets only recognized lab tables and indexes.

The lab commands verify the target database is named `querystorm`. `lab init` creates or resets the lab tables `querystorm_inventory` and `querystorm_orders`, including truncating and reseeding the orders table. Use a disposable database; do not point it at a database containing application data or tables with those names. Run `lab init --seed 42` to restore the deterministic dataset.

Set both `DATABASE_URL` and `MONITOR_DATABASE_URL` when connecting to a custom lab. The monitor role is separate from workload traffic. The demo supports `inventory-hotspot`, `pool-starvation`, and `index-contention`. Deadlock reproduction and reservation contention are not available yet.

Mutating and workload commands reject non-loopback database hosts by default. `--allow-remote` is available only for an explicitly disposable remote test database; the command still requires the database name `querystorm`.

## Security and data handling

The Compose lab uses fixed sample passwords so it can start without provisioning credentials. They are for the disposable local lab only and must never be reused. The database port and dashboard bind to loopback by default. The dashboard has no authentication; do not bind it to a public or shared network. Reports contain measurement and PostgreSQL activity metadata, so review them before sharing. Query text is not included in `pg_stat_statements` report entries.

## Reading measurements

The workload is closed-loop: each worker starts its next operation after the previous one finishes. Its throughput is not independently configured offered load. Pool acquisition is client-side waiting; SQL round-trip time includes driver and network effects and is not PostgreSQL internal execution time. Request percentiles include both successful operations and returned errors. Session samples are point-in-time observations, so short waits may fall between samples and sampled wait observations are not exact total wait durations. Database counters are scoped to the whole lab database and include collector overhead. Statement statistics are grouped by PostgreSQL's database, role, query ID, and top-level flag. They omit SQL text and can include other sessions using the workload role; PostgreSQL may also publish in-progress cumulative stats with delay. Request percentiles come from individual request observations, not statement averages.

The inventory row is restored within each committed transaction, and the report checks it remains at 1,000,000 and nonnegative. Throughput varies by machine; no latency threshold or improvement is assumed.

## Commands

```text
postgres-concurrency-tester doctor
postgres-concurrency-tester lab init [--seed N]
postgres-concurrency-tester demo [--scenario inventory-hotspot|pool-starvation|index-contention] [--indexed] [--concurrency N] [--seconds N] [--pool-size N] [--hold-ms N] [--output DIR]
postgres-concurrency-tester matrix [--scenario inventory-hotspot|pool-starvation|index-contention] [--concurrency 1,8,16,32] [--pool-sizes 4,16] [--repetitions N]
postgres-concurrency-tester compare BASELINE.json CANDIDATE.json [--max-p95-regression-percent N] [--max-error-rate-percent N] [--strict]
postgres-concurrency-tester serve [--directory DIR] [--listen IP:PORT]
```

`compare` accepts one run JSON file or a directory containing exactly one run JSON file for each side. Pool size and hold interval may differ and are surfaced as configuration differences; scenario, concurrency, duration, and correctness must remain comparable.

The server listens on localhost by default and serves `/` plus JSON run history at `/api/runs`.

## Development

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for development and contribution instructions. Security issues should be reported privately; see [SECURITY.md](./.github/SECURITY.md).

## License

MIT; see [LICENSE](./LICENSE).

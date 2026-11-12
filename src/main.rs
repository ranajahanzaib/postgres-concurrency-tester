use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use axum::{Json, Router, extract::State, routing::get};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    Connection, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::str::FromStr;
use tokio::{sync::Mutex, task::JoinSet, time::sleep};
use uuid::Uuid;

mod monitor;
use monitor::{
    DatabaseCounterDelta, MonitorSummary, counter_delta, database_counters, sample_activity,
};
mod statements;
use statements::{StatementStatsDelta, delta as statement_delta, snapshot as statement_snapshot};

#[derive(Parser)]
#[command(
    name = "postgres-concurrency-tester",
    about = "Find database failures caused by concurrency, not just slow SQL"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Doctor,
    #[command(subcommand)]
    Lab(LabCommand),
    #[command(alias = "run")]
    Demo {
        #[arg(long, value_enum, default_value_t = Scenario::InventoryHotspot)]
        scenario: Scenario,
        #[arg(
            long,
            help = "Build the composite orders index for the index-contention scenario"
        )]
        indexed: bool,
        #[arg(
            long,
            help = "Allow a non-loopback PostgreSQL target; only use with an explicitly disposable database"
        )]
        allow_remote: bool,
        #[arg(
            long,
            env = "DATABASE_URL",
            default_value = "postgres://querystorm_app:querystorm_app@localhost:55432/querystorm"
        )]
        database_url: String,
        #[arg(
            long,
            env = "MONITOR_DATABASE_URL",
            default_value = "postgres://querystorm_monitor:querystorm_monitor@localhost:55432/querystorm"
        )]
        monitor_database_url: String,
        #[arg(long, default_value_t = 16)]
        concurrency: usize,
        #[arg(long, default_value_t = 10)]
        seconds: u64,
        #[arg(long, default_value_t = 8)]
        pool_size: u32,
        #[arg(long, default_value_t = 5)]
        hold_ms: u64,
        #[arg(long, default_value = "runs")]
        output: PathBuf,
    },
    Matrix {
        #[arg(long, value_enum, default_value_t = Scenario::InventoryHotspot)]
        scenario: Scenario,
        #[arg(
            long,
            help = "Build the composite orders index for the index-contention scenario"
        )]
        indexed: bool,
        #[arg(
            long,
            env = "DATABASE_URL",
            default_value = "postgres://querystorm_app:querystorm_app@localhost:55432/querystorm"
        )]
        database_url: String,
        #[arg(
            long,
            env = "MONITOR_DATABASE_URL",
            default_value = "postgres://querystorm_monitor:querystorm_monitor@localhost:55432/querystorm"
        )]
        monitor_database_url: String,
        #[arg(long, value_delimiter = ',', default_value = "1,8,16,32")]
        concurrency: Vec<usize>,
        #[arg(long, value_delimiter = ',', default_value = "4,16")]
        pool_sizes: Vec<u32>,
        #[arg(long, default_value_t = 1)]
        repetitions: u32,
        #[arg(long, default_value_t = 10)]
        seconds: u64,
        #[arg(long, default_value_t = 5)]
        hold_ms: u64,
        #[arg(long, default_value = "runs/matrix")]
        output: PathBuf,
        #[arg(long)]
        allow_remote: bool,
    },
    Serve {
        #[arg(long, default_value = "runs")]
        directory: PathBuf,
        #[arg(long, default_value = "127.0.0.1:8787")]
        listen: String,
    },
    Compare {
        baseline: PathBuf,
        candidate: PathBuf,
        #[arg(long)]
        max_p95_regression_percent: Option<f64>,
        #[arg(long)]
        max_error_rate_percent: Option<f64>,
        #[arg(long)]
        strict: bool,
    },
}
#[derive(Clone, Copy, Debug, ValueEnum)]
enum Scenario {
    InventoryHotspot,
    PoolStarvation,
    IndexContention,
}
impl Scenario {
    fn id(self) -> &'static str {
        match self {
            Self::InventoryHotspot => "inventory-hotspot",
            Self::PoolStarvation => "pool-starvation",
            Self::IndexContention => "index-contention",
        }
    }
}
#[derive(Subcommand)]
enum LabCommand {
    Init {
        #[arg(
            long,
            env = "DATABASE_URL",
            default_value = "postgres://querystorm_app:querystorm_app@localhost:55432/querystorm"
        )]
        database_url: String,
        #[arg(long, default_value_t = 42)]
        seed: i64,
        #[arg(
            long,
            help = "Allow a non-loopback PostgreSQL target; only use with an explicitly disposable database"
        )]
        allow_remote: bool,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct RunReport {
    id: Uuid,
    scenario: String,
    started_at: DateTime<Utc>,
    duration_seconds: u64,
    actual_duration_ms: u64,
    cancelled: bool,
    concurrency: usize,
    pool_max: u32,
    hold_ms: u64,
    indexed: bool,
    attempts: u64,
    completed: u64,
    errors: u64,
    pool_timeouts: u64,
    deadlock_errors: u64,
    pool_wait_dominant_requests: u64,
    throughput_per_second: f64,
    request_latency_ms: Quantiles,
    pool_acquire_ms: Quantiles,
    sql_round_trip_ms: Quantiles,
    correctness: Correctness,
    monitoring: MonitorSummary,
    database_counters: Option<DatabaseCounterDelta>,
    statement_stats: Option<StatementStatsDelta>,
    findings: Vec<Finding>,
    query_plan: Option<Value>,
    index_candidate: bool,
    note: String,
}
#[derive(Debug, Serialize, Deserialize)]
struct Quantiles {
    samples: u64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}
#[derive(Debug, Serialize, Deserialize)]
struct Correctness {
    inventory_nonnegative: bool,
    inventory_restored: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Finding {
    id: String,
    category: String,
    severity: String,
    confidence: String,
    title: String,
    summary: String,
    evidence: Vec<String>,
    limitations: Vec<String>,
    suggested_actions: Vec<String>,
}
#[derive(Clone, Copy)]
struct DemoConfig {
    scenario: Scenario,
    concurrency: usize,
    seconds: u64,
    pool_max: u32,
    hold_ms: u64,
    indexed: bool,
}
struct MonitoringBaseline {
    database: Option<monitor::DatabaseCounters>,
    statements: Option<statements::StatementSnapshot>,
    workload_user_oid: i64,
    query_plan: Option<Value>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    match Cli::parse().command {
        Command::Doctor => doctor().await,
        Command::Lab(LabCommand::Init {
            database_url,
            seed,
            allow_remote,
        }) => {
            ensure_local_target(&database_url, allow_remote)?;
            let pool = connect(&database_url, 2).await?;
            ensure_lab_database(&pool).await?;
            initialize(&pool, seed).await?;
            println!("Laboratory initialized (seed {seed}).");
            Ok(())
        }
        Command::Demo {
            scenario,
            indexed,
            allow_remote,
            database_url,
            monitor_database_url,
            concurrency,
            seconds,
            pool_size,
            hold_ms,
            output,
        } => {
            if indexed && !matches!(scenario, Scenario::IndexContention) {
                bail!("--indexed is only valid with --scenario index-contention");
            }
            if concurrency == 0
                || concurrency > 256
                || seconds == 0
                || seconds > 3600
                || pool_size == 0
                || pool_size > 128
                || hold_ms > 10_000
            {
                bail!(
                    "limits: concurrency 1..=256, seconds 1..=3600, pool_size 1..=128, hold_ms <= 10000"
                );
            }
            ensure_local_target(&database_url, allow_remote)?;
            ensure_local_target(&monitor_database_url, allow_remote)?;
            let report = execute_run(
                &database_url,
                &monitor_database_url,
                DemoConfig {
                    scenario,
                    concurrency,
                    seconds,
                    pool_max: pool_size,
                    hold_ms,
                    indexed,
                },
                &output,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            println!("Reports written to {}", output.display());
            Ok(())
        }
        Command::Matrix {
            scenario,
            indexed,
            database_url,
            monitor_database_url,
            concurrency,
            pool_sizes,
            repetitions,
            seconds,
            hold_ms,
            output,
            allow_remote,
        } => {
            if indexed && !matches!(scenario, Scenario::IndexContention) {
                bail!("--indexed is only valid with --scenario index-contention");
            }
            if concurrency.is_empty()
                || pool_sizes.is_empty()
                || repetitions == 0
                || repetitions > 10
                || seconds == 0
                || seconds > 3600
                || hold_ms > 10_000
                || concurrency.iter().any(|&n| n == 0 || n > 256)
                || pool_sizes.iter().any(|&n| n == 0 || n > 128)
            {
                bail!(
                    "matrix limits: concurrency 1..=256, pool size 1..=128, repetitions 1..=10, seconds 1..=3600, hold_ms <= 10000"
                );
            }
            let cases = concurrency
                .len()
                .saturating_mul(pool_sizes.len())
                .saturating_mul(repetitions as usize);
            let total_seconds = cases as u64 * seconds;
            if cases > 64 || total_seconds > 3600 {
                bail!("matrix is bounded to 64 cases and 3600 total measurement seconds");
            }
            let mut seen_concurrency = concurrency.clone();
            seen_concurrency.sort_unstable();
            seen_concurrency.dedup();
            let mut seen_pools = pool_sizes.clone();
            seen_pools.sort_unstable();
            seen_pools.dedup();
            if seen_concurrency.len() != concurrency.len() || seen_pools.len() != pool_sizes.len() {
                bail!("matrix concurrency and pool-size lists cannot contain duplicates");
            }
            ensure_local_target(&database_url, allow_remote)?;
            ensure_local_target(&monitor_database_url, allow_remote)?;
            let mut completed = 0;
            for rep in 1..=repetitions {
                for &concurrency in &concurrency {
                    for &pool_size in &pool_sizes {
                        let case_dir = output.join(format!(
                            "{}-c{concurrency}-p{pool_size}-r{rep}-{}",
                            scenario.id(),
                            Uuid::new_v4()
                        ));
                        let report = execute_run(
                            &database_url,
                            &monitor_database_url,
                            DemoConfig {
                                scenario,
                                concurrency,
                                seconds,
                                pool_max: pool_size,
                                hold_ms,
                                indexed,
                            },
                            &case_dir,
                        )
                        .await?;
                        completed += 1;
                        println!(
                            "{}/{} {} concurrency={} pool={} throughput={:.2}/s p95={:.3}ms errors={} report={}",
                            completed,
                            cases,
                            scenario.id(),
                            concurrency,
                            pool_size,
                            report.throughput_per_second,
                            report.request_latency_ms.p95,
                            report.errors,
                            case_dir.display()
                        );
                    }
                }
            }
            Ok(())
        }
        Command::Serve { directory, listen } => serve(directory, &listen).await,
        Command::Compare {
            baseline,
            candidate,
            max_p95_regression_percent,
            max_error_rate_percent,
            strict,
        } => {
            let exit = compare_command(
                &baseline,
                &candidate,
                max_p95_regression_percent,
                max_error_rate_percent,
                strict,
            )?;
            if exit != 0 {
                std::process::exit(exit);
            }
            Ok(())
        }
    }
}

async fn connect(url: &str, max: u32) -> Result<PgPool> {
    connect_named(url, max, "querystorm-workload").await
}
async fn connect_named(url: &str, max: u32, application_name: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(url)?
        .application_name(application_name)
        .options([
            ("statement_timeout", "5s"),
            ("lock_timeout", "3s"),
            ("idle_in_transaction_session_timeout", "15s"),
        ]);
    PgPoolOptions::new()
        .max_connections(max)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options)
        .await
        .context("connecting to PostgreSQL; start lab/compose.yaml and check DATABASE_URL")
}
async fn execute_run(
    database_url: &str,
    monitor_database_url: &str,
    config: DemoConfig,
    output: &std::path::Path,
) -> Result<RunReport> {
    let pool = connect(database_url, config.pool_max).await?;
    let monitor_pool = connect_named(monitor_database_url, 2, "querystorm-monitor").await?;
    ensure_lab_database(&pool).await?;
    ensure_lab_database(&monitor_pool).await?;
    check_lab(&pool).await?;
    let query_plan = if matches!(config.scenario, Scenario::IndexContention) {
        prepare_index_scenario(&pool, config.indexed).await?;
        Some(capture_order_plan(&pool).await?)
    } else {
        None
    };
    let counters_before = database_counters(&monitor_pool).await.ok();
    let workload_user_oid = sqlx::query_scalar::<_, i64>(
        "SELECT oid::bigint FROM pg_roles WHERE rolname = current_user",
    )
    .fetch_one(&pool)
    .await?;
    let statement_stats_before = statement_snapshot(&monitor_pool, workload_user_oid)
        .await
        .ok();
    let report = run_demo(
        pool,
        monitor_pool,
        MonitoringBaseline {
            database: counters_before,
            statements: statement_stats_before,
            workload_user_oid,
            query_plan,
        },
        config,
    )
    .await?;
    write_report(&report, output)?;
    Ok(report)
}

async fn prepare_index_scenario(pool: &PgPool, indexed: bool) -> Result<()> {
    if indexed {
        sqlx::query("CREATE INDEX IF NOT EXISTS querystorm_orders_tenant_customer_created_idx ON querystorm_orders (tenant_id, customer_id, created_at DESC)")
            .execute(pool).await?;
    } else {
        sqlx::query("DROP INDEX IF EXISTS querystorm_orders_tenant_customer_created_idx")
            .execute(pool)
            .await?;
    }
    sqlx::query("ANALYZE querystorm_orders")
        .execute(pool)
        .await?;
    Ok(())
}

async fn capture_order_plan(pool: &PgPool) -> Result<Value> {
    let row = sqlx::query("EXPLAIN (FORMAT JSON) SELECT id, status, created_at FROM querystorm_orders WHERE tenant_id = $1 AND customer_id = $2 ORDER BY created_at DESC LIMIT 20")
        .bind(17_i32)
        .bind(37_i32)
        .fetch_one(pool)
        .await?;
    let plan: sqlx::types::Json<Value> = row.try_get(0)?;
    Ok(plan.0)
}
fn ensure_local_target(url: &str, allow_remote: bool) -> Result<()> {
    let host = PgConnectOptions::from_str(url)?.get_host().to_owned();
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host.starts_with('/')
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if !loopback && !allow_remote {
        bail!(
            "refusing non-loopback PostgreSQL host `{host}`; pass --allow-remote only when the target is a disposable test database"
        );
    }
    Ok(())
}
async fn doctor() -> Result<()> {
    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgres://querystorm_app:querystorm_app@localhost:55432/querystorm".into()
    });
    let pool = connect(&url, 1).await?;
    let version: String = sqlx::query_scalar("SELECT version()")
        .fetch_one(&pool)
        .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    let ext: Option<String> = sqlx::query_scalar(
        "SELECT extversion FROM pg_extension WHERE extname='pg_stat_statements'",
    )
    .fetch_optional(&pool)
    .await?;
    let preloaded: bool = sqlx::query_scalar(
        "SELECT position('pg_stat_statements' in current_setting('shared_preload_libraries')) > 0",
    )
    .fetch_one(&pool)
    .await?;
    let query_id: String = sqlx::query_scalar("SELECT current_setting('compute_query_id')")
        .fetch_one(&pool)
        .await?;
    let lab_schema: bool =
        sqlx::query_scalar("SELECT to_regclass('public.querystorm_inventory') IS NOT NULL")
            .fetch_one(&pool)
            .await?;
    let pg_stat_io: bool =
        sqlx::query_scalar("SELECT to_regclass('pg_catalog.pg_stat_io') IS NOT NULL")
            .fetch_one(&pool)
            .await?;
    println!("Database: {database}\nPostgreSQL: {version}");
    println!(
        "{} pg_stat_statements preload",
        if preloaded { "[ok]" } else { "[missing]" }
    );
    println!(
        "{} query identifier calculation ({query_id})",
        if query_id != "off" {
            "[ok]"
        } else {
            "[missing]"
        }
    );
    println!(
        "{} pg_stat_statements extension ({})",
        if ext.is_some() { "[ok]" } else { "[missing]" },
        ext.unwrap_or_else(|| "not installed".into())
    );
    println!(
        "{} inventory laboratory schema",
        if lab_schema { "[ok]" } else { "[missing]" }
    );
    println!(
        "{} optional pg_stat_io",
        if pg_stat_io { "[ok]" } else { "[unavailable]" }
    );
    let monitor_url = std::env::var("MONITOR_DATABASE_URL").unwrap_or_else(|_| {
        "postgres://querystorm_monitor:querystorm_monitor@localhost:55432/querystorm".into()
    });
    match connect_named(&monitor_url, 1, "querystorm-monitor-doctor").await {
        Ok(monitor_pool) => {
            let can_read: bool =
                sqlx::query_scalar("SELECT pg_has_role(current_user, 'pg_monitor', 'MEMBER')")
                    .fetch_one(&monitor_pool)
                    .await?;
            println!(
                "{} separate monitoring connection and pg_monitor",
                if can_read { "[ok]" } else { "[missing]" }
            );
        }
        Err(_) => println!("[missing] separate monitoring connection (check MONITOR_DATABASE_URL)"),
    }
    Ok(())
}
async fn initialize(pool: &PgPool, seed: i64) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS querystorm_inventory (product_id BIGINT PRIMARY KEY, quantity BIGINT NOT NULL CHECK (quantity >= 0), seed BIGINT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())").execute(&mut *tx).await?;
    sqlx::query("INSERT INTO querystorm_inventory(product_id, quantity, seed) VALUES (1, 1000000, $1) ON CONFLICT(product_id) DO UPDATE SET quantity=1000000, seed=EXCLUDED.seed, updated_at=now()").bind(seed).execute(&mut *tx).await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS querystorm_orders (id BIGINT PRIMARY KEY, tenant_id INTEGER NOT NULL, customer_id INTEGER NOT NULL, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, total_cents INTEGER NOT NULL)")
        .execute(&mut *tx).await?;
    sqlx::query("DROP INDEX IF EXISTS querystorm_orders_tenant_customer_created_idx")
        .execute(&mut *tx)
        .await?;
    sqlx::query("TRUNCATE TABLE querystorm_orders")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO querystorm_orders(id, tenant_id, customer_id, status, created_at, total_cents) SELECT i, ((i * 17 + $1) % 1000)::integer, ((i * 37 + $1) % 5000)::integer, CASE i % 4 WHEN 0 THEN 'paid' WHEN 1 THEN 'open' WHEN 2 THEN 'cancelled' ELSE 'shipped' END, TIMESTAMPTZ '2020-01-01 00:00:00+00' + i * INTERVAL '1 second', (i * 313) % 500000 FROM generate_series(1, 50000) AS g(i)")
        .bind(seed).execute(&mut *tx).await?;
    sqlx::query("ANALYZE querystorm_orders")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
async fn ensure_lab_database(pool: &PgPool) -> Result<()> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;
    if database != "querystorm" {
        bail!(
            "refusing to initialize database `{database}`; the disposable lab must be named `querystorm`"
        );
    }
    Ok(())
}
async fn check_lab(pool: &PgPool) -> Result<()> {
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public.querystorm_inventory') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    if !exists {
        bail!("laboratory not initialized; run `postgres-concurrency-tester lab init` first");
    }
    Ok(())
}
async fn run_demo(
    pool: PgPool,
    monitor_pool: PgPool,
    baseline: MonitoringBaseline,
    config: DemoConfig,
) -> Result<RunReport> {
    let DemoConfig {
        scenario,
        concurrency,
        seconds,
        pool_max,
        hold_ms,
        indexed,
    } = config;
    let MonitoringBaseline {
        database: counters_before,
        statements: statement_stats_before,
        workload_user_oid,
        query_plan,
    } = baseline;
    let start_wall = Utc::now();
    let started = Instant::now();
    let until = started + Duration::from_secs(seconds);
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel_handler = {
        let cancelled = cancelled.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                cancelled.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        })
    };
    let monitor_task = tokio::spawn(sample_activity(
        monitor_pool.clone(),
        Duration::from_secs(seconds),
        200,
        cancelled.clone(),
    ));
    let mut tasks = JoinSet::new();
    let req = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));
    let acquire = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));
    let sql = Arc::new(Mutex::new(Histogram::<u64>::new(3)?));
    let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let completed = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let errors = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let pool_timeouts = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let deadlock_errors = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let pool_wait_dominant = Arc::new(std::sync::atomic::AtomicU64::new(0));
    for _ in 0..concurrency {
        let pool = pool.clone();
        let req = req.clone();
        let acquire = acquire.clone();
        let sql = sql.clone();
        let attempts = attempts.clone();
        let completed = completed.clone();
        let errors = errors.clone();
        let pool_timeouts = pool_timeouts.clone();
        let deadlock_errors = deadlock_errors.clone();
        let pool_wait_dominant = pool_wait_dominant.clone();
        let cancelled = cancelled.clone();
        tasks.spawn(async move {
            while Instant::now() < until && !cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let request_start = Instant::now();
                let acq_start = Instant::now();
                let mut conn = match pool.acquire().await {
                    Ok(c) => c,
                    Err(error) => {
                        let request_us = request_start.elapsed().as_micros() as u64;
                        let acq_us = acq_start.elapsed().as_micros() as u64;
                        req.lock().await.record(request_us).ok();
                        acquire.lock().await.record(acq_us).ok();
                        if acq_us > 0 && acq_us.saturating_mul(2) >= request_us {
                            pool_wait_dominant.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if matches!(error, sqlx::Error::PoolTimedOut) {
                            pool_timeouts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        continue;
                    }
                };
                let acq_us = acq_start.elapsed().as_micros() as u64;
                let operation = async {
                    if matches!(scenario, Scenario::IndexContention) {
                        let q_start = Instant::now();
                        sqlx::query("SELECT id, status, created_at FROM querystorm_orders WHERE tenant_id = $1 AND customer_id = $2 ORDER BY created_at DESC LIMIT 20")
                            .bind(17_i32)
                            .bind(37_i32)
                            .fetch_all(&mut *conn)
                            .await?;
                        let query_us = q_start.elapsed().as_micros() as u64;
                        return Ok::<(u64, Option<u64>), sqlx::Error>((query_us, None));
                    }
                    let mut tx = Connection::begin(&mut *conn).await?;
                    let q_start = Instant::now();
                    sqlx::query("UPDATE querystorm_inventory SET quantity=quantity-1, updated_at=now() WHERE product_id=1 AND quantity>0").execute(&mut *tx).await?;
                    let q1_us = q_start.elapsed().as_micros() as u64;
                    if hold_ms > 0 && matches!(scenario, Scenario::InventoryHotspot) { sleep(Duration::from_millis(hold_ms)).await; }
                    let q_start = Instant::now();
                    sqlx::query("UPDATE querystorm_inventory SET quantity=quantity+1, updated_at=now() WHERE product_id=1").execute(&mut *tx).await?;
                    let q2_us = q_start.elapsed().as_micros() as u64;
                    tx.commit().await?; Ok::<(u64, Option<u64>), sqlx::Error>((q1_us, Some(q2_us)))
                }.await;
                if operation.is_ok() && matches!(scenario, Scenario::PoolStarvation) && hold_ms > 0 {
                    sleep(Duration::from_millis(hold_ms)).await;
                }
                let request_us = request_start.elapsed().as_micros() as u64;
                if acq_us > 0 && acq_us.saturating_mul(2) >= request_us {
                    pool_wait_dominant.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                acquire.lock().await.record(acq_us).ok();
                req.lock().await.record(request_us).ok();
                match operation {
                    Ok((first_sql_us, second_sql_us)) => {
                        completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        {
                            let mut histogram = sql.lock().await;
                            histogram.record(first_sql_us).ok();
                            if let Some(sample) = second_sql_us { histogram.record(sample).ok(); }
                        }
                    }
                    Err(error) => {
                        errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if error.as_database_error().and_then(|e| e.code()).as_deref() == Some("40P01") {
                            deadlock_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            }
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.context("workload worker panicked")?;
    }
    cancel_handler.abort();
    let attempts = attempts.load(std::sync::atomic::Ordering::Relaxed);
    let completed_n = completed.load(std::sync::atomic::Ordering::Relaxed);
    let errors_n = errors.load(std::sync::atomic::Ordering::Relaxed);
    let pool_timeouts_n = pool_timeouts.load(std::sync::atomic::Ordering::Relaxed);
    let deadlock_errors_n = deadlock_errors.load(std::sync::atomic::Ordering::Relaxed);
    let pool_wait_dominant_n = pool_wait_dominant.load(std::sync::atomic::Ordering::Relaxed);
    let (inventory, quantity): (i64, i64) =
        sqlx::query_as("SELECT seed, quantity FROM querystorm_inventory WHERE product_id=1")
            .fetch_one(&pool)
            .await?;
    let _ = inventory;
    let monitoring = monitor_task.await.context("monitoring task panicked")?;
    let database_counters = match (counters_before, database_counters(&monitor_pool).await.ok()) {
        (Some(before), Some(after)) => Some(counter_delta(&before, &after)),
        _ => None,
    };
    let statement_stats = match (
        statement_stats_before,
        statement_snapshot(&monitor_pool, workload_user_oid)
            .await
            .ok(),
    ) {
        (Some(before), Some(after)) => Some(statement_delta(&before, &after)),
        _ => None,
    };
    let actual_duration = started.elapsed();
    let elapsed = actual_duration.as_secs_f64();
    let note = match scenario {
        Scenario::InventoryHotspot => {
            "Closed-loop workload against one shared inventory row. The configured hold occurs inside the transaction. SQL round trips are client-observed durations; sampled lock activity is incomplete and does not measure exact wait duration."
        }
        Scenario::PoolStarvation => {
            "Closed-loop workload holds each acquired application connection for the configured interval after commit. This creates client-side pool waiting while minimizing row-lock time. Database lock observations remain separately reported."
        }
        Scenario::IndexContention => {
            "Closed-loop indexed-read workload over a deterministic orders table. EXPLAIN (FORMAT JSON) was captured without ANALYZE; the plan describes planner estimates, while request timing comes from executed SELECT statements."
        }
    };
    let mut report = RunReport {
        id: Uuid::new_v4(),
        scenario: scenario.id().into(),
        started_at: start_wall,
        duration_seconds: seconds,
        actual_duration_ms: actual_duration.as_millis().min(u64::MAX as u128) as u64,
        cancelled: cancelled.load(std::sync::atomic::Ordering::Relaxed),
        concurrency,
        pool_max,
        hold_ms,
        indexed,
        attempts,
        completed: completed_n,
        errors: errors_n,
        pool_timeouts: pool_timeouts_n,
        deadlock_errors: deadlock_errors_n,
        pool_wait_dominant_requests: pool_wait_dominant_n,
        throughput_per_second: completed_n as f64 / elapsed,
        request_latency_ms: quantiles(req.lock().await.clone()),
        pool_acquire_ms: quantiles(acquire.lock().await.clone()),
        sql_round_trip_ms: quantiles(sql.lock().await.clone()),
        correctness: Correctness {
            inventory_nonnegative: quantity >= 0,
            inventory_restored: quantity == 1_000_000,
        },
        monitoring,
        database_counters,
        statement_stats,
        findings: Vec::new(),
        query_plan,
        index_candidate: indexed,
        note: note.into(),
    };
    report.findings = analyze(&report);
    Ok(report)
}

fn analyze(report: &RunReport) -> Vec<Finding> {
    let mut findings = Vec::new();
    if report.attempts >= 10
        && report.pool_wait_dominant_requests as f64 / report.attempts as f64 >= 0.30
    {
        findings.push(Finding {
            id: "pool-acquisition-dominates-some-requests".into(),
            category: "connection_pool".into(),
            severity: "warning".into(),
            confidence: "medium".into(),
            title: "Connection acquisition took at least half of request time for many attempts".into(),
            summary: "The per-request client measurements show connection-pool acquisition accounted for at least half of total request time in 30% or more of attempts.".into(),
            evidence: vec![format!("{} of {} attempts met the paired timing threshold; pool acquire p95 was {:.3} ms and request p95 was {:.3} ms.", report.pool_wait_dominant_requests, report.attempts, report.pool_acquire_ms.p95, report.request_latency_ms.p95)],
            limitations: vec!["This identifies client-side pool queueing evidence and does not establish whether increasing pool size is safe for the database.".into()],
            suggested_actions: vec!["Review connection hold time and pool sizing together with database capacity.".into()],
        });
    }
    if report.monitoring.lock_wait_samples > 0 {
        findings.push(Finding {
            id: "sampled-lock-wait".into(),
            category: "database_locking".into(),
            severity: "warning".into(),
            confidence: "high".into(),
            title: "PostgreSQL lock waits were observed".into(),
            summary: "At least one PostgreSQL Concurrency Tester workload backend was sampled with a PostgreSQL Lock wait event.".into(),
            evidence: vec![format!("{} lock-wait observations and {} blocking edges were recorded across {} monitor samples.", report.monitoring.lock_wait_samples, report.monitoring.blocking_edges_observed, report.monitoring.samples)],
            limitations: report.monitoring.limitations.clone(),
            suggested_actions: vec!["Inspect the sampled waiter/blocker PIDs and transaction ages; shorten critical sections where semantics permit.".into()],
        });
    }
    if report.deadlock_errors > 0 {
        findings.push(Finding {
            id: "postgres-deadlock-confirmed".into(),
            category: "deadlock".into(),
            severity: "critical".into(),
            confidence: "high".into(),
            title: "PostgreSQL reported a deadlock".into(),
            summary: "A workload operation failed with SQLSTATE 40P01, which confirms a PostgreSQL deadlock error.".into(),
            evidence: vec![format!("{} workload operation(s) returned SQLSTATE 40P01.", report.deadlock_errors)],
            limitations: vec!["The current inventory and pool-starvation workloads do not intentionally construct a deadlock; this finding appears only if PostgreSQL returns that SQLSTATE.".into()],
            suggested_actions: vec!["Use a consistent lock acquisition order and only retry transactions whose business operation is safe to repeat.".into()],
        });
    }
    let maximum_transaction_age = report
        .monitoring
        .observations
        .iter()
        .filter_map(|s| s.transaction_age_ms)
        .max()
        .unwrap_or_default();
    if report.monitoring.idle_in_transaction_samples > 0
        || maximum_transaction_age > report.hold_ms as i64 + 500
    {
        findings.push(Finding {
            id: "long-transaction-observed".into(),
            category: "transaction_lifetime".into(),
            severity: "warning".into(),
            confidence: "medium".into(),
            title: "A long or idle-in-transaction workload session was sampled".into(),
            summary: "PostgreSQL session samples include an idle transaction or a transaction older than the configured intentional hold plus 500 ms.".into(),
            evidence: vec![format!("Maximum sampled transaction age: {maximum_transaction_age} ms; idle-in-transaction observations: {}.", report.monitoring.idle_in_transaction_samples)],
            limitations: report.monitoring.limitations.clone(),
            suggested_actions: vec!["Check transaction scope and avoid application work while holding database locks.".into()],
        });
    }
    if !report.correctness.inventory_nonnegative || !report.correctness.inventory_restored {
        findings.push(Finding {
            id: "correctness-regression".into(),
            category: "correctness".into(),
            severity: "critical".into(),
            confidence: "high".into(),
            title: "Business invariant failed".into(),
            summary: "This result must be rejected regardless of its performance measurements."
                .into(),
            evidence: vec![format!(
                "inventory_nonnegative={}, inventory_restored={}",
                report.correctness.inventory_nonnegative, report.correctness.inventory_restored
            )],
            limitations: vec![],
            suggested_actions: vec![
                "Fix the transactional invariant before comparing performance.".into(),
            ],
        });
    }
    findings
}
fn quantiles(h: Histogram<u64>) -> Quantiles {
    let ms = |p: f64| h.value_at_quantile(p) as f64 / 1000.0;
    Quantiles {
        samples: h.len(),
        p50: ms(0.50),
        p95: ms(0.95),
        p99: ms(0.99),
        max: h.max() as f64 / 1000.0,
    }
}

fn write_report(r: &RunReport, dir: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    std::fs::write(
        dir.join(format!("{}.json", r.id)),
        serde_json::to_vec_pretty(r)?,
    )?;
    let deadlock_delta = r
        .database_counters
        .as_ref()
        .filter(|c| c.valid)
        .and_then(|c| c.deadlocks)
        .map(|n| n.to_string())
        .unwrap_or_else(|| "unavailable or invalid".into());
    let (statement_status, statement_groups, statement_calls, statement_exec_ms) =
        match &r.statement_stats {
            Some(stats) if stats.valid => (
                "valid".to_owned(),
                stats.entries.len(),
                stats
                    .entries
                    .iter()
                    .map(|e| e.delta.calls)
                    .fold(0_i64, i64::saturating_add),
                stats
                    .entries
                    .iter()
                    .map(|e| e.delta.total_exec_time_ms)
                    .sum::<f64>(),
            ),
            Some(stats) => (
                stats.reason.clone().unwrap_or_else(|| "invalid".into()),
                0,
                0,
                0.0,
            ),
            None => ("unavailable".into(), 0, 0, 0.0),
        };
    let findings = if r.findings.is_empty() {
        "No diagnostic rule matched the recorded evidence.".into()
    } else {
        r.findings
            .iter()
            .map(|f| format!("- **{}** ({}): {}", f.title, f.severity, f.summary))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let captured_plan_nodes = r
        .query_plan
        .as_ref()
        .map(|plan| plan_nodes(plan).join(" → "))
        .unwrap_or_else(|| "not captured".into());
    let md = format!(
        "# PostgreSQL Concurrency Tester run {}\n\n- Scenario: {}\n- Indexed candidate: {}\n- Captured plan nodes: {}\n- Workload: closed-loop, {} workers for {} seconds\n- Requests: {} completed / {} attempts ({} errors, {} pool timeouts, {} PostgreSQL deadlock errors)\n- Throughput: {:.2} requests/s\n- Request latency p50/p95/p99: {:.3}/{:.3}/{:.3} ms ({} samples)\n- Pool acquisition p50/p95/p99: {:.3}/{:.3}/{:.3} ms\n- SQL round-trip p50/p95/p99: {:.3}/{:.3}/{:.3} ms\n- Pool wait dominated {} attempts\n- Monitor samples: {} ({} query errors; {} observations retained)\n- Sampled lock-wait observations: {} ({} blocking edges observed)\n- Idle-in-transaction observations: {}\n- Database deadlock counter delta: {}\n- pg_stat_statements delta: {} ({} statement groups, {} calls, {:.3} ms execution time)\n- Correctness: nonnegative={}, restored={}\n\n## Findings\n\n{}\n\n{}\n",
        r.id,
        r.scenario,
        r.index_candidate,
        captured_plan_nodes,
        r.concurrency,
        r.duration_seconds,
        r.completed,
        r.attempts,
        r.errors,
        r.pool_timeouts,
        r.deadlock_errors,
        r.pool_wait_dominant_requests,
        r.throughput_per_second,
        r.request_latency_ms.p50,
        r.request_latency_ms.p95,
        r.request_latency_ms.p99,
        r.request_latency_ms.samples,
        r.pool_acquire_ms.p50,
        r.pool_acquire_ms.p95,
        r.pool_acquire_ms.p99,
        r.sql_round_trip_ms.p50,
        r.sql_round_trip_ms.p95,
        r.sql_round_trip_ms.p99,
        r.monitoring.samples,
        r.monitoring.query_errors,
        r.monitoring.observations.len(),
        r.monitoring.lock_wait_samples,
        r.monitoring.blocking_edges_observed,
        r.monitoring.idle_in_transaction_samples,
        deadlock_delta,
        statement_status,
        statement_groups,
        statement_calls,
        statement_exec_ms,
        r.correctness.inventory_nonnegative,
        r.correctness.inventory_restored,
        findings,
        r.note,
    );
    std::fs::write(dir.join(format!("{}.md", r.id)), md)?;
    Ok(())
}

#[derive(Debug, Serialize)]
struct MetricComparison {
    baseline: f64,
    candidate: f64,
    change_percent: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ComparisonReport {
    status: String,
    baseline_id: Uuid,
    candidate_id: Uuid,
    reasons: Vec<String>,
    configuration_differences: Vec<String>,
    request_p50_ms: MetricComparison,
    request_p95_ms: MetricComparison,
    request_p99_ms: MetricComparison,
    throughput_per_second: MetricComparison,
    pool_acquire_p95_ms: MetricComparison,
    sql_round_trip_p95_ms: MetricComparison,
    error_rate_percent: Option<MetricComparison>,
    pool_timeouts: MetricComparison,
    deadlock_errors: MetricComparison,
    sampled_lock_wait_observations: Option<MetricComparison>,
    query_plan: Option<PlanComparison>,
    candidate_error_rate_percent: Option<f64>,
    correctness: bool,
    regression_gates_passed: bool,
}

#[derive(Debug, Serialize)]
struct PlanComparison {
    structural_change: bool,
    baseline_nodes: Vec<String>,
    candidate_nodes: Vec<String>,
}

fn normalized_plan(plan: &Value) -> Value {
    fn normalize(value: &mut Value) {
        match value {
            Value::Object(object) => {
                for key in [
                    "Startup Cost",
                    "Total Cost",
                    "Plan Rows",
                    "Plan Width",
                    "Planning Time",
                ] {
                    object.remove(key);
                }
                for child in object.values_mut() {
                    normalize(child);
                }
            }
            Value::Array(items) => {
                for child in items {
                    normalize(child);
                }
            }
            _ => {}
        }
    }
    let mut normalized = plan.clone();
    normalize(&mut normalized);
    normalized
}

fn plan_nodes(plan: &Value) -> Vec<String> {
    fn walk(value: &Value, output: &mut Vec<String>) {
        if let Some(object) = value.as_object() {
            if let Some(node) = object.get("Node Type").and_then(Value::as_str) {
                output.push(match object.get("Index Name").and_then(Value::as_str) {
                    Some(index) => format!("{node} ({index})"),
                    None => node.to_owned(),
                });
            }
            for child in object.values() {
                walk(child, output);
            }
        } else if let Some(array) = value.as_array() {
            for child in array {
                walk(child, output);
            }
        }
    }
    let mut output = Vec::new();
    walk(plan, &mut output);
    output
}

fn plan_comparison(baseline: &Option<Value>, candidate: &Option<Value>) -> Option<PlanComparison> {
    let (Some(baseline), Some(candidate)) = (baseline, candidate) else {
        return None;
    };
    Some(PlanComparison {
        structural_change: normalized_plan(baseline) != normalized_plan(candidate),
        baseline_nodes: plan_nodes(baseline),
        candidate_nodes: plan_nodes(candidate),
    })
}

fn metric_comparison(baseline: f64, candidate: f64) -> MetricComparison {
    MetricComparison {
        baseline,
        candidate,
        change_percent: if baseline == 0.0 {
            None
        } else {
            Some((candidate - baseline) / baseline * 100.0)
        },
    }
}

fn compare_reports(baseline: &RunReport, candidate: &RunReport) -> ComparisonReport {
    let mut reasons: Vec<String> = Vec::new();
    let mut configuration_differences: Vec<String> = Vec::new();
    let configuration_valid = baseline.scenario == candidate.scenario
        && baseline.concurrency == candidate.concurrency
        && baseline.duration_seconds == candidate.duration_seconds;
    if !configuration_valid {
        reasons.push("Scenario, concurrency, or duration differs.".into());
    }
    if baseline.pool_max != candidate.pool_max {
        configuration_differences.push(format!(
            "pool_max: {} -> {}",
            baseline.pool_max, candidate.pool_max
        ));
    }
    if baseline.hold_ms != candidate.hold_ms {
        configuration_differences.push(format!(
            "hold_ms: {} -> {}",
            baseline.hold_ms, candidate.hold_ms
        ));
    }
    if baseline.index_candidate != candidate.index_candidate {
        configuration_differences.push(format!(
            "indexed: {} -> {}",
            baseline.index_candidate, candidate.index_candidate
        ));
    }
    let correctness = baseline.correctness.inventory_nonnegative
        && baseline.correctness.inventory_restored
        && candidate.correctness.inventory_nonnegative
        && candidate.correctness.inventory_restored;
    if !correctness {
        reasons.push("At least one run failed its inventory correctness invariant.".into());
    }
    if baseline.cancelled || candidate.cancelled {
        reasons.push(
            "At least one run was cancelled before its configured measurement duration ended."
                .into(),
        );
    }
    if baseline.request_latency_ms.samples == 0 || candidate.request_latency_ms.samples == 0 {
        reasons.push("A run has no completed request latency samples.".into());
    }
    if baseline.request_latency_ms.p95 == 0.0 {
        reasons.push("Baseline p95 is zero, so percentage latency change is undefined.".into());
    }
    if baseline.monitoring.query_errors > 0
        || baseline.monitoring.samples == 0
        || candidate.monitoring.query_errors > 0
        || candidate.monitoring.samples == 0
    {
        reasons.push("PostgreSQL monitoring coverage is incomplete in at least one run.".into());
    }
    if baseline.statement_stats.as_ref().is_none_or(|s| !s.valid)
        || candidate.statement_stats.as_ref().is_none_or(|s| !s.valid)
    {
        reasons
            .push("pg_stat_statements delta is unavailable or invalid in at least one run.".into());
    }
    let has_request_samples =
        baseline.request_latency_ms.samples > 0 && candidate.request_latency_ms.samples > 0;
    let candidate_error_rate_percent = if candidate.attempts > 0 {
        Some(candidate.errors as f64 / candidate.attempts as f64 * 100.0)
    } else {
        None
    };
    let baseline_error_rate_percent = if baseline.attempts > 0 {
        Some(baseline.errors as f64 / baseline.attempts as f64 * 100.0)
    } else {
        None
    };
    let invalid = !configuration_valid
        || !correctness
        || !has_request_samples
        || baseline.cancelled
        || candidate.cancelled;
    ComparisonReport {
        status: if invalid {
            "INVALID".into()
        } else if reasons.is_empty() {
            "VALID".into()
        } else {
            "PARTIALLY_COMPARABLE".into()
        },
        baseline_id: baseline.id,
        candidate_id: candidate.id,
        reasons,
        configuration_differences,
        request_p50_ms: metric_comparison(
            baseline.request_latency_ms.p50,
            candidate.request_latency_ms.p50,
        ),
        request_p95_ms: metric_comparison(
            baseline.request_latency_ms.p95,
            candidate.request_latency_ms.p95,
        ),
        request_p99_ms: metric_comparison(
            baseline.request_latency_ms.p99,
            candidate.request_latency_ms.p99,
        ),
        throughput_per_second: metric_comparison(
            baseline.throughput_per_second,
            candidate.throughput_per_second,
        ),
        pool_acquire_p95_ms: metric_comparison(
            baseline.pool_acquire_ms.p95,
            candidate.pool_acquire_ms.p95,
        ),
        sql_round_trip_p95_ms: metric_comparison(
            baseline.sql_round_trip_ms.p95,
            candidate.sql_round_trip_ms.p95,
        ),
        error_rate_percent: baseline_error_rate_percent
            .zip(candidate_error_rate_percent)
            .map(|(b, c)| metric_comparison(b, c)),
        pool_timeouts: metric_comparison(
            baseline.pool_timeouts as f64,
            candidate.pool_timeouts as f64,
        ),
        deadlock_errors: metric_comparison(
            baseline.deadlock_errors as f64,
            candidate.deadlock_errors as f64,
        ),
        sampled_lock_wait_observations: (baseline.monitoring.samples > 0
            && candidate.monitoring.samples > 0)
            .then(|| {
                metric_comparison(
                    baseline.monitoring.lock_wait_samples as f64,
                    candidate.monitoring.lock_wait_samples as f64,
                )
            }),
        query_plan: plan_comparison(&baseline.query_plan, &candidate.query_plan),
        candidate_error_rate_percent,
        correctness,
        regression_gates_passed: true,
    }
}

fn load_report(path: &std::path::Path) -> Result<RunReport> {
    let path = if path.is_dir() {
        let mut reports = std::fs::read_dir(path)?
            .flatten()
            .map(|entry| entry.path())
            .filter(|p| p.extension().is_some_and(|x| x == "json"))
            .collect::<Vec<_>>();
        reports.sort();
        if reports.len() != 1 {
            bail!(
                "{} must contain exactly one JSON run report",
                path.display()
            );
        }
        reports.remove(0)
    } else {
        path.to_path_buf()
    };
    let bytes =
        std::fs::read(&path).with_context(|| format!("reading report {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing report {}", path.display()))
}

fn compare_command(
    baseline_path: &std::path::Path,
    candidate_path: &std::path::Path,
    max_p95_regression_percent: Option<f64>,
    max_error_rate_percent: Option<f64>,
    strict: bool,
) -> Result<i32> {
    for threshold in [max_p95_regression_percent, max_error_rate_percent]
        .into_iter()
        .flatten()
    {
        if !threshold.is_finite() || threshold < 0.0 {
            bail!("regression thresholds must be finite nonnegative percentages");
        }
    }
    let baseline = load_report(baseline_path)?;
    let candidate = load_report(candidate_path)?;
    let mut comparison = compare_reports(&baseline, &candidate);
    let mut exceeded = false;
    let mut gate_unavailable = false;
    if let Some(maximum) = max_p95_regression_percent {
        if let Some(change) = comparison.request_p95_ms.change_percent {
            if change > maximum {
                exceeded = true;
                comparison.reasons.push(format!(
                    "p95 regression {change:.2}% exceeded the {maximum:.2}% limit."
                ));
            }
        } else {
            gate_unavailable = true;
            comparison
                .reasons
                .push("p95 regression gate cannot run because baseline p95 is zero.".into());
        }
    }
    if let Some(maximum) = max_error_rate_percent {
        if let Some(rate) = comparison.candidate_error_rate_percent {
            if rate > maximum {
                exceeded = true;
                comparison.reasons.push(format!(
                    "Candidate error rate {rate:.2}% exceeded the {maximum:.2}% limit."
                ));
            }
        } else {
            gate_unavailable = true;
            comparison
                .reasons
                .push("Error-rate gate cannot run because the candidate has no attempts.".into());
        }
    }
    comparison.regression_gates_passed = !exceeded && !gate_unavailable;
    println!("{}", serde_json::to_string_pretty(&comparison)?);
    if comparison.status == "INVALID"
        || gate_unavailable
        || (strict && comparison.status != "VALID")
    {
        Ok(3)
    } else if exceeded {
        Ok(2)
    } else {
        Ok(0)
    }
}

async fn serve(dir: PathBuf, listen: &str) -> Result<()> {
    #[derive(Clone)]
    struct AppState(PathBuf);
    async fn runs(State(s): State<AppState>) -> Json<Vec<serde_json::Value>> {
        let mut out: Vec<serde_json::Value> = vec![];
        if let Ok(entries) = std::fs::read_dir(s.0) {
            for e in entries
                .flatten()
                .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
            {
                if let Ok(bytes) = std::fs::read(e.path())
                    && let Ok(v) = serde_json::from_slice(&bytes)
                {
                    out.push(v);
                }
            }
        }
        out.sort_by_key(|report| {
            report
                .get("started_at")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned()
        });
        Json(out)
    }
    async fn run_by_id(
        State(state): State<AppState>,
        axum::extract::Path(id): axum::extract::Path<String>,
    ) -> Result<Json<serde_json::Value>, axum::http::StatusCode> {
        let id = Uuid::parse_str(&id).map_err(|_| axum::http::StatusCode::NOT_FOUND)?;
        let bytes = std::fs::read(state.0.join(format!("{id}.json")))
            .map_err(|_| axum::http::StatusCode::NOT_FOUND)?;
        let report = serde_json::from_slice(&bytes)
            .map_err(|_| axum::http::StatusCode::INTERNAL_SERVER_ERROR)?;
        Ok(Json(report))
    }
    const DASHBOARD: &str = r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>PostgreSQL Concurrency Tester · concurrency lab</title>
<style>
:root{color-scheme:dark;--bg:#0b1017;--panel:#111a25;--line:#243243;--muted:#91a2b7;--text:#e9f0f7;--cyan:#48d6c0;--blue:#76a8ff;--amber:#f2bd66;--red:#ff7887}*{box-sizing:border-box}body{margin:0;background:radial-gradient(ellipse at 50% -25%,#1b3340 0,transparent 48%),var(--bg);color:var(--text);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}header{height:74px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 max(24px,calc((100vw - 1440px)/2));background:#0c131ddd;position:sticky;top:0;z-index:2;backdrop-filter:blur(12px)}.brand{display:flex;align-items:center;gap:13px;font-size:15px;font-weight:700;letter-spacing:.02em}.mark{width:32px;height:32px;border:1px solid #397b79;border-radius:9px;display:grid;place-items:center;color:var(--cyan);font-size:18px}.eyebrow,.muted{color:var(--muted)}.eyebrow{text-transform:uppercase;font-size:10px;letter-spacing:.16em;font-weight:700}.wrap{max-width:1440px;margin:auto;padding:38px 28px 60px}.title-row{display:flex;align-items:flex-end;justify-content:space-between;gap:20px;margin-bottom:26px}.title-row h1{font-size:29px;letter-spacing:-.04em;margin:5px 0 4px}.title-row p{margin:0;color:var(--muted)}button,select{font:inherit;color:var(--text);background:#14202d;border:1px solid #34485e;border-radius:8px;padding:9px 13px}button{cursor:pointer}button:hover{border-color:var(--cyan)}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}.card,.panel{background:linear-gradient(145deg,#121d29,#101823);border:1px solid var(--line);border-radius:12px}.card{padding:18px 19px;min-height:108px}.card .label{font-size:12px;color:var(--muted)}.card .value{font-size:25px;font-weight:650;letter-spacing:-.04em;margin-top:7px}.card .unit{font-size:13px;color:var(--muted);font-weight:500;letter-spacing:0}.columns{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(330px,.85fr);gap:16px;margin-top:16px}.panel{padding:20px}.panel-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:18px}.panel h2{font-size:14px;margin:0;font-weight:650}.subhead{font-size:12px;color:var(--muted);margin-top:3px}.chart{width:100%;height:245px}.chart text{fill:var(--muted);font-size:11px}.chart .gridline{stroke:#273748;stroke-width:1}.chart .series{fill:none;stroke:var(--cyan);stroke-width:2.5}.chart .dot{fill:var(--bg);stroke:var(--cyan);stroke-width:2.5}.row{display:grid;grid-template-columns:1fr auto;gap:20px;padding:12px 0;border-bottom:1px solid #22303e}.row:last-child{border-bottom:0}.row strong{font-weight:600}.badge{font-size:11px;border:1px solid #35495c;color:var(--muted);padding:3px 8px;border-radius:999px}.badge.good{color:var(--cyan);border-color:#27605c}.badge.warn{color:var(--amber);border-color:#71542f}.badge.bad{color:var(--red);border-color:#743943}.table-wrap{overflow:auto}table{width:100%;border-collapse:collapse;white-space:nowrap}th,td{text-align:left;padding:11px 12px;border-bottom:1px solid #22303e}th{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.1em}tbody tr{cursor:pointer}tbody tr:hover{background:#182534}tbody tr[aria-selected="true"]{background:#17312f}.empty{padding:28px 0;color:var(--muted);text-align:center}.bottom{margin-top:16px}.error{color:var(--red)}.controls{display:flex;gap:10px;align-items:center}.controls label{color:var(--muted);font-size:12px}@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}.columns{grid-template-columns:1fr}}@media(max-width:560px){header{padding:0 16px}.wrap{padding:26px 15px}.title-row{align-items:flex-start;flex-direction:column}.grid{gap:9px}.card{padding:13px;min-height:96px}.card .value{font-size:21px}.panel{padding:15px}.columns{gap:10px}}
</style></head><body><header><div class="brand"><span class="mark" aria-hidden="true">⌁</span><span>QUERYSTORM</span></div><div class="eyebrow">POSTGRESQL CONCURRENCY LAB</div></header>
<main class="wrap"><div class="title-row"><div><div class="eyebrow">EXPERIMENT REVIEW</div><h1>Concurrency, made visible.</h1><p>Measured request behavior, client pool waiting, and PostgreSQL evidence.</p></div><div class="controls"><label for="runSelect">Selected run</label><select id="runSelect" aria-label="Select an experiment run"></select><button id="refresh">Refresh</button></div></div>
<section class="grid" aria-label="Selected run measurements"><article class="card"><div class="label">Request p50</div><div class="value" id="p50">—</div></article><article class="card"><div class="label">Request p95</div><div class="value" id="p95">—</div></article><article class="card"><div class="label">Request p99</div><div class="value" id="p99">—</div></article><article class="card"><div class="label">Throughput</div><div class="value" id="throughput">—</div></article></section>
<div class="columns"><section class="panel"><div class="panel-head"><div><h2>Request p95 by concurrency</h2><div class="subhead">Each point is a completed local experiment; no interpolated results.</div></div><span class="badge" id="curveLabel">No runs</span></div><svg id="curve" class="chart" viewBox="0 0 720 245" role="img" aria-label="Measured request p95 by concurrency"></svg><div class="muted" id="curveEmpty" hidden>No completed runs are available yet.</div></section>
<section class="panel"><div class="panel-head"><div><h2>Selected experiment</h2><div class="subhead" id="runMeta">Choose a run from history.</div></div><span class="badge" id="correctness">—</span></div><div id="details"></div><div class="eyebrow" style="margin-top:20px">EVIDENCE-BASED FINDINGS</div><div id="findings"></div></section></div>
<section class="panel bottom"><div class="panel-head"><div><h2>Run history</h2><div class="subhead">Click a row to inspect its recorded measurements.</div></div><span class="badge" id="runCount">0 runs</span></div><div class="table-wrap"><table><thead><tr><th>Scenario</th><th>Concurrency</th><th>Pool</th><th>p95</th><th>Throughput</th><th>Errors</th><th>Lock samples</th><th>Started</th></tr></thead><tbody id="history"></tbody></table><div id="empty" class="empty">No real run artifacts found in the selected report directory.</div></div></section><p id="error" class="error" role="alert"></p></main>
<script>
const $=id=>document.getElementById(id), runs=[];let selected=null;const fmt=(x,n=2)=>Number.isFinite(Number(x))?Number(x).toFixed(n):"—", text=(id,value)=>$(id).textContent=value;
function detailRow(label,value){const row=document.createElement('div');row.className='row';const a=document.createElement('span');a.className='muted';a.textContent=label;const b=document.createElement('strong');b.textContent=value;row.append(a,b);return row}
function renderFindings(r){const host=$('findings');host.replaceChildren();const fs=r.findings||[];if(!fs.length){host.append(detailRow('Findings','No rule matched the recorded evidence.'));return}for(const f of fs){const row=detailRow(f.title,f.severity);host.append(row);const summary=document.createElement('div');summary.className='subhead';summary.style.padding='0 0 10px';summary.textContent=f.summary;host.append(summary)}}
function render(r){if(!r)return;selected=r;text('p50',fmt(r.request_latency_ms?.p50)+' ms');text('p95',fmt(r.request_latency_ms?.p95)+' ms');text('p99',fmt(r.request_latency_ms?.p99)+' ms');text('throughput',fmt(r.throughput_per_second)+' ops/s');text('runMeta',r.scenario+' · '+r.concurrency+' workers · pool '+r.pool_max);const pass=r.correctness?.inventory_nonnegative&&r.correctness?.inventory_restored;text('correctness',pass?'INVARIANTS PASS':'INVARIANT FAILURE');$('correctness').className='badge '+(pass?'good':'bad');const d=$('details');d.replaceChildren();d.append(detailRow('SQL round-trip p95',fmt(r.sql_round_trip_ms?.p95)+' ms'));d.append(detailRow('Pool acquire p95',fmt(r.pool_acquire_ms?.p95)+' ms'));d.append(detailRow('Pool timeouts',String(r.pool_timeouts??'—')));d.append(detailRow('Pool-dominant requests',String(r.pool_wait_dominant_requests??'—')));d.append(detailRow('Sampled lock waits',String(r.monitoring?.lock_wait_samples??'unavailable')));d.append(detailRow('Database active samples',String(r.monitoring?.samples??'unavailable')));d.append(detailRow('Deadlock SQLSTATE 40P01',String(r.deadlock_errors??'—')));d.append(detailRow('pg_stat_statements',r.statement_stats?.valid?'delta captured':'unavailable / invalid'));renderFindings(r)}
function drawCurve(){const svg=$('curve');svg.replaceChildren();const data=runs.filter(r=>r.request_latency_ms?.samples>0&&selected&&r.scenario===selected.scenario&&r.pool_max===selected.pool_max&&r.hold_ms===selected.hold_ms&&r.index_candidate===selected.index_candidate).sort((a,b)=>a.concurrency-b.concurrency);$('curveEmpty').hidden=data.length>0;$('curveLabel').textContent=data.length?data.length+' comparable points':'No comparable runs';if(!data.length)return;const w=720,h=245,left=54,right=18,top=12,bottom=31,max=Math.max(...data.map(r=>r.request_latency_ms.p95),1);for(let i=0;i<=4;i++){const y=top+(h-top-bottom)*i/4;const line=document.createElementNS('http://www.w3.org/2000/svg','line');line.setAttribute('x1',left);line.setAttribute('x2',w-right);line.setAttribute('y1',y);line.setAttribute('y2',y);line.setAttribute('class','gridline');svg.append(line);const label=document.createElementNS('http://www.w3.org/2000/svg','text');label.setAttribute('x',left-8);label.setAttribute('y',y+4);label.setAttribute('text-anchor','end');label.textContent=fmt(max*(4-i)/4,1);svg.append(label)}const pts=data.map((r,i)=>{const x=left+(data.length===1?(w-left-right)/2:(w-left-right)*i/(data.length-1));const y=top+(h-top-bottom)*(1-r.request_latency_ms.p95/max);return [x,y,r]});const line=document.createElementNS('http://www.w3.org/2000/svg','polyline');line.setAttribute('points',pts.map(p=>p[0]+','+p[1]).join(' '));line.setAttribute('class','series');svg.append(line);for(const [x,y,r] of pts){const c=document.createElementNS('http://www.w3.org/2000/svg','circle');c.setAttribute('cx',x);c.setAttribute('cy',y);c.setAttribute('r',5);c.setAttribute('class','dot');const title=document.createElementNS('http://www.w3.org/2000/svg','title');title.textContent=r.concurrency+' workers · '+fmt(r.request_latency_ms.p95)+' ms · '+r.scenario;c.append(title);svg.append(c)} }
function renderHistory(){const body=$('history');body.replaceChildren();$('empty').hidden=runs.length>0;text('runCount',runs.length+' runs');const select=$('runSelect');select.replaceChildren();for(const r of runs){const option=document.createElement('option');option.value=r.id;option.textContent=r.scenario+' · '+r.concurrency+' workers · '+new Date(r.started_at).toLocaleString();select.append(option);const tr=document.createElement('tr');tr.tabIndex=0;tr.setAttribute('aria-selected',String(selected?.id===r.id));const vals=[r.scenario,r.concurrency,r.pool_max,fmt(r.request_latency_ms?.p95)+' ms',fmt(r.throughput_per_second)+' /s',r.errors,r.monitoring?.lock_wait_samples??'—',new Date(r.started_at).toLocaleString()];for(const v of vals){const td=document.createElement('td');td.textContent=String(v);tr.append(td)}tr.addEventListener('click',()=>{select.value=r.id;render(r);renderHistory()});tr.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();tr.click()}});body.append(tr)}if(selected)select.value=selected.id}
async function load(){text('error','');try{const response=await fetch('/api/runs');if(!response.ok)throw new Error('Report API returned '+response.status);runs.splice(0,runs.length,...await response.json());runs.sort((a,b)=>new Date(b.started_at)-new Date(a.started_at));selected=runs.find(r=>r.id===selected?.id)||runs[0]||null;renderHistory();drawCurve();render(selected)}catch(e){text('error','Could not load run artifacts: '+e.message)}}$('refresh').addEventListener('click',load);$('runSelect').addEventListener('change',e=>{const run=runs.find(r=>r.id===e.target.value);render(run);renderHistory();drawCurve()});load();
</script></body></html>"##;
    let app = Router::new()
        .route("/api/runs", get(runs))
        .route("/api/runs/{id}", get(run_by_id))
        .route("/", get(|| async { axum::response::Html(DASHBOARD) }))
        .with_state(AppState(dir));
    let listener = tokio::net::TcpListener::bind(listen).await?;
    println!("Dashboard/API listening at http://{listen}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_are_derived_from_observed_samples() {
        let mut histogram = Histogram::<u64>::new(3).unwrap();
        for value in [1_000, 2_000, 3_000, 4_000, 100_000] {
            histogram.record(value).unwrap();
        }
        let q = quantiles(histogram);
        assert_eq!(q.samples, 5);
        assert!((q.p50 - 3.0).abs() < 0.01);
        assert!(q.p95 >= 99.0);
        assert!(q.max >= 100.0 && q.max < 100.1);
    }

    #[test]
    fn report_round_trips_through_json() {
        let report = RunReport {
            id: Uuid::nil(),
            scenario: "inventory-hotspot".into(),
            started_at: Utc::now(),
            duration_seconds: 1,
            actual_duration_ms: 1000,
            cancelled: false,
            concurrency: 1,
            pool_max: 1,
            hold_ms: 0,
            indexed: false,
            attempts: 2,
            completed: 2,
            errors: 0,
            pool_timeouts: 0,
            deadlock_errors: 0,
            pool_wait_dominant_requests: 0,
            throughput_per_second: 2.0,
            request_latency_ms: Quantiles {
                samples: 2,
                p50: 1.0,
                p95: 1.0,
                p99: 1.0,
                max: 1.0,
            },
            pool_acquire_ms: Quantiles {
                samples: 2,
                p50: 0.1,
                p95: 0.1,
                p99: 0.1,
                max: 0.2,
            },
            sql_round_trip_ms: Quantiles {
                samples: 2,
                p50: 0.5,
                p95: 0.5,
                p99: 0.5,
                max: 1.0,
            },
            correctness: Correctness {
                inventory_nonnegative: true,
                inventory_restored: true,
            },
            monitoring: MonitorSummary::default(),
            database_counters: None,
            statement_stats: None,
            findings: vec![],
            query_plan: None,
            index_candidate: false,
            note: "test".into(),
        };
        let encoded = serde_json::to_vec(&report).unwrap();
        let decoded: RunReport = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.completed, 2);
        assert!(decoded.correctness.inventory_restored);
        assert!(analyze(&decoded).is_empty());
        let mut candidate: RunReport = serde_json::from_slice(&encoded).unwrap();
        candidate.pool_max = 4;
        let comparison = compare_reports(&decoded, &candidate);
        assert_eq!(comparison.status, "PARTIALLY_COMPARABLE");
        assert_eq!(
            comparison.configuration_differences,
            vec!["pool_max: 1 -> 4"]
        );
        candidate.correctness.inventory_restored = false;
        assert_eq!(compare_reports(&decoded, &candidate).status, "INVALID");
        let mut evidence = decoded;
        evidence.monitoring.samples = 4;
        evidence.monitoring.lock_wait_samples = 2;
        evidence.deadlock_errors = 1;
        let findings = analyze(&evidence);
        assert!(findings.iter().any(|f| f.id == "sampled-lock-wait"));
        assert!(
            findings
                .iter()
                .any(|f| f.id == "postgres-deadlock-confirmed")
        );
    }

    #[test]
    fn plan_comparison_ignores_estimate_changes_but_reports_structural_changes() {
        let baseline = serde_json::json!([{"Plan":{"Node Type":"Seq Scan","Plan Rows":500,"Total Cost":900.0}}]);
        let same_shape =
            serde_json::json!([{"Plan":{"Node Type":"Seq Scan","Plan Rows":20,"Total Cost":12.0}}]);
        let index_shape = serde_json::json!([{"Plan":{"Node Type":"Index Scan","Index Name":"orders_idx","Plan Rows":20}}]);
        assert!(
            !plan_comparison(&Some(baseline.clone()), &Some(same_shape))
                .unwrap()
                .structural_change
        );
        let changed = plan_comparison(&Some(baseline), &Some(index_shape)).unwrap();
        assert!(changed.structural_change);
        assert_eq!(changed.candidate_nodes, vec!["Index Scan (orders_idx)"]);
    }
}

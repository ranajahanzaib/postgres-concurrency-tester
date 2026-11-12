use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use tokio::time::{MissedTickBehavior, interval, timeout};

const MAX_SESSION_OBSERVATIONS: usize = 20_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionObservation {
    pub observed_at: DateTime<Utc>,
    pub pid: i32,
    pub application_name: String,
    pub state: Option<String>,
    pub wait_event_type: Option<String>,
    pub wait_event: Option<String>,
    pub query_start: Option<DateTime<Utc>>,
    pub transaction_start: Option<DateTime<Utc>>,
    pub transaction_age_ms: Option<i64>,
    pub blockers: Vec<i32>,
    pub lock_type: Option<String>,
    pub lock_mode: Option<String>,
    pub relation: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MonitorSummary {
    pub interval_ms: u64,
    pub samples: u64,
    pub query_errors: u64,
    pub lock_wait_samples: u64,
    pub blocking_edges_observed: u64,
    pub idle_in_transaction_samples: u64,
    pub truncated: bool,
    pub observations: Vec<SessionObservation>,
    pub limitations: Vec<String>,
}

pub async fn sample_activity(
    pool: PgPool,
    duration: Duration,
    interval_ms: u64,
    cancelled: Arc<AtomicBool>,
) -> MonitorSummary {
    let mut summary = MonitorSummary {
        interval_ms,
        ..MonitorSummary::default()
    };
    let deadline = Instant::now() + duration;
    let mut ticker = interval(Duration::from_millis(interval_ms.max(50)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    while Instant::now() < deadline && !cancelled.load(Ordering::Relaxed) {
        ticker.tick().await;
        if Instant::now() >= deadline {
            break;
        }
        let observed_at = Utc::now();
        let query = sqlx::query(
            "SELECT a.pid, a.application_name, a.state, a.wait_event_type, a.wait_event, \
                    a.query_start, a.xact_start, \
                    CASE WHEN a.xact_start IS NULL THEN NULL \
                         ELSE GREATEST(0, (extract(epoch FROM clock_timestamp() - a.xact_start) * 1000)::bigint) END AS transaction_age_ms, \
                    pg_blocking_pids(a.pid) AS blockers, l.locktype, l.mode AS lock_mode, \
                    CASE WHEN l.relation IS NULL THEN NULL ELSE l.relation::regclass::text END AS relation \
             FROM pg_stat_activity AS a \
             LEFT JOIN LATERAL (SELECT locktype, mode, relation FROM pg_locks \
                                WHERE pid = a.pid AND NOT granted LIMIT 1) AS l ON true \
             WHERE a.datname = current_database() \
               AND a.application_name LIKE 'querystorm-workload%' \
               AND a.pid <> pg_backend_pid() \
             ORDER BY a.pid",
        );
        let rows = match timeout(Duration::from_millis(500), query.fetch_all(&pool)).await {
            Ok(Ok(rows)) => rows,
            _ => {
                summary.query_errors += 1;
                continue;
            }
        };
        summary.samples += 1;
        for row in rows {
            let mut blockers: Vec<i32> = row.try_get("blockers").unwrap_or_default();
            blockers.sort_unstable();
            blockers.dedup();
            let state: Option<String> = row.try_get("state").ok().flatten();
            let wait_event_type: Option<String> = row.try_get("wait_event_type").ok().flatten();
            if wait_event_type.as_deref() == Some("Lock") {
                summary.lock_wait_samples += 1;
                summary.blocking_edges_observed += blockers.len() as u64;
            }
            if matches!(
                state.as_deref(),
                Some("idle in transaction" | "idle in transaction (aborted)")
            ) {
                summary.idle_in_transaction_samples += 1;
            }
            if summary.observations.len() >= MAX_SESSION_OBSERVATIONS {
                summary.truncated = true;
                continue;
            }
            summary.observations.push(SessionObservation {
                observed_at,
                pid: row.try_get("pid").unwrap_or_default(),
                application_name: row.try_get("application_name").unwrap_or_default(),
                state,
                wait_event_type,
                wait_event: row.try_get("wait_event").ok().flatten(),
                query_start: row.try_get("query_start").ok().flatten(),
                transaction_start: row.try_get("xact_start").ok().flatten(),
                transaction_age_ms: row.try_get("transaction_age_ms").ok().flatten(),
                blockers,
                lock_type: row.try_get("locktype").ok().flatten(),
                lock_mode: row.try_get("lock_mode").ok().flatten(),
                relation: row.try_get("relation").ok().flatten(),
            });
        }
    }
    summary.limitations.push(
        "Lock wait samples show states observed at sampling instants; they are not complete or exact wait durations. Short waits may fall between samples.".into(),
    );
    summary.limitations.push(
        "Blocking PIDs are deduplicated per observation. PID 0 can represent a prepared transaction; sampled graph cycles are not proof of a PostgreSQL deadlock.".into(),
    );
    summary
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseCounters {
    pub stats_reset: Option<DateTime<Utc>>,
    pub commits: i64,
    pub rollbacks: i64,
    pub deadlocks: i64,
    pub blocks_read: i64,
    pub blocks_hit: i64,
}

pub async fn database_counters(pool: &PgPool) -> Result<DatabaseCounters> {
    let row = sqlx::query(
        "SELECT stats_reset, xact_commit, xact_rollback, deadlocks, blks_read, blks_hit \
         FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await?;
    Ok(DatabaseCounters {
        stats_reset: row.try_get("stats_reset")?,
        commits: row.try_get("xact_commit")?,
        rollbacks: row.try_get("xact_rollback")?,
        deadlocks: row.try_get("deadlocks")?,
        blocks_read: row.try_get("blks_read")?,
        blocks_hit: row.try_get("blks_hit")?,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseCounterDelta {
    pub valid: bool,
    pub commits: Option<i64>,
    pub rollbacks: Option<i64>,
    pub deadlocks: Option<i64>,
    pub blocks_read: Option<i64>,
    pub blocks_hit: Option<i64>,
    pub limitation: String,
}

pub fn counter_delta(before: &DatabaseCounters, after: &DatabaseCounters) -> DatabaseCounterDelta {
    let pairs = [
        (before.commits, after.commits),
        (before.rollbacks, after.rollbacks),
        (before.deadlocks, after.deadlocks),
        (before.blocks_read, after.blocks_read),
        (before.blocks_hit, after.blocks_hit),
    ];
    let valid = before.stats_reset == after.stats_reset && pairs.iter().all(|(a, b)| b >= a);
    let delta = |a: i64, b: i64| valid.then_some(b - a);
    DatabaseCounterDelta {
        valid,
        commits: delta(before.commits, after.commits),
        rollbacks: delta(before.rollbacks, after.rollbacks),
        deadlocks: delta(before.deadlocks, after.deadlocks),
        blocks_read: delta(before.blocks_read, after.blocks_read),
        blocks_hit: delta(before.blocks_hit, after.blocks_hit),
        limitation: if valid {
            "Database-scoped counter deltas may include unrelated sessions using the same database."
                .into()
        } else {
            "Counter deltas are invalid because the statistics reset timestamp changed or a counter decreased.".into()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counters(reset: DateTime<Utc>, commits: i64) -> DatabaseCounters {
        DatabaseCounters {
            stats_reset: Some(reset),
            commits,
            rollbacks: 0,
            deadlocks: 0,
            blocks_read: 0,
            blocks_hit: 0,
        }
    }

    #[test]
    fn counter_delta_rejects_reset_or_decrease() {
        let t = Utc::now();
        assert_eq!(
            counter_delta(&counters(t, 4), &counters(t, 9)).commits,
            Some(5)
        );
        assert!(!counter_delta(&counters(t, 9), &counters(t, 4)).valid);
        assert!(
            !counter_delta(
                &counters(t, 4),
                &counters(t + chrono::Duration::seconds(1), 9)
            )
            .valid
        );
    }
}

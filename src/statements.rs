use std::collections::BTreeMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};

#[derive(Debug, Clone)]
pub struct StatementSnapshot {
    pub reset_at: Option<DateTime<Utc>>,
    pub deallocations: i64,
    pub track_planning: Option<bool>,
    pub statements: BTreeMap<StatementKey, StatementCounters>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StatementKey {
    pub dbid: i64,
    pub userid: i64,
    pub queryid: Option<i64>,
    pub toplevel: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementCounters {
    pub calls: i64,
    pub plans: i64,
    pub total_plan_time_ms: f64,
    pub mean_plan_time_ms: f64,
    pub min_plan_time_ms: f64,
    pub max_plan_time_ms: f64,
    pub stddev_plan_time_ms: f64,
    pub total_exec_time_ms: f64,
    pub mean_exec_time_ms: f64,
    pub min_exec_time_ms: f64,
    pub max_exec_time_ms: f64,
    pub stddev_exec_time_ms: f64,
    pub rows: i64,
    pub shared_blks_hit: i64,
    pub shared_blks_read: i64,
    pub shared_blks_dirtied: i64,
    pub shared_blks_written: i64,
    pub temp_blks_read: i64,
    pub temp_blks_written: i64,
    pub wal_bytes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementDeltaEntry {
    pub key: StatementKey,
    pub delta: StatementCounters,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementStatsDelta {
    pub valid: bool,
    pub reason: Option<String>,
    pub track_planning: Option<bool>,
    pub entries: Vec<StatementDeltaEntry>,
    pub limitation: String,
}

pub async fn snapshot(pool: &PgPool, workload_user_oid: i64) -> Result<StatementSnapshot> {
    let info = sqlx::query("SELECT stats_reset, dealloc FROM pg_stat_statements_info")
        .fetch_one(pool)
        .await?;
    let reset_at = info.try_get("stats_reset")?;
    let deallocations = info.try_get("dealloc")?;
    let planning_setting: Option<String> =
        sqlx::query_scalar("SELECT current_setting('pg_stat_statements.track_planning', true)")
            .fetch_one(pool)
            .await?;
    let track_planning = planning_setting.map(|s| s == "on");
    let rows = sqlx::query(
        "SELECT dbid::bigint AS dbid, userid::bigint AS userid, queryid, toplevel, \
                calls, plans, total_plan_time, mean_plan_time, min_plan_time, max_plan_time, stddev_plan_time, \
                total_exec_time, mean_exec_time, min_exec_time, max_exec_time, stddev_exec_time, rows, \
                shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, \
                temp_blks_read, temp_blks_written, wal_bytes::text AS wal_bytes \
         FROM pg_stat_statements \
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) \
           AND userid = $1",
    )
    .bind(workload_user_oid)
    .fetch_all(pool)
    .await?;
    let mut statements = BTreeMap::new();
    for row in rows {
        let key = StatementKey {
            dbid: row.try_get("dbid")?,
            userid: row.try_get("userid")?,
            queryid: row.try_get("queryid")?,
            toplevel: row.try_get("toplevel")?,
        };
        statements.insert(
            key,
            StatementCounters {
                calls: row.try_get("calls")?,
                plans: row.try_get("plans")?,
                total_plan_time_ms: row.try_get("total_plan_time")?,
                mean_plan_time_ms: row.try_get("mean_plan_time")?,
                min_plan_time_ms: row.try_get("min_plan_time")?,
                max_plan_time_ms: row.try_get("max_plan_time")?,
                stddev_plan_time_ms: row.try_get("stddev_plan_time")?,
                total_exec_time_ms: row.try_get("total_exec_time")?,
                mean_exec_time_ms: row.try_get("mean_exec_time")?,
                min_exec_time_ms: row.try_get("min_exec_time")?,
                max_exec_time_ms: row.try_get("max_exec_time")?,
                stddev_exec_time_ms: row.try_get("stddev_exec_time")?,
                rows: row.try_get("rows")?,
                shared_blks_hit: row.try_get("shared_blks_hit")?,
                shared_blks_read: row.try_get("shared_blks_read")?,
                shared_blks_dirtied: row.try_get("shared_blks_dirtied")?,
                shared_blks_written: row.try_get("shared_blks_written")?,
                temp_blks_read: row.try_get("temp_blks_read")?,
                temp_blks_written: row.try_get("temp_blks_written")?,
                wal_bytes: row.try_get("wal_bytes")?,
            },
        );
    }
    Ok(StatementSnapshot {
        reset_at,
        deallocations,
        track_planning,
        statements,
    })
}

pub fn delta(before: &StatementSnapshot, after: &StatementSnapshot) -> StatementStatsDelta {
    let reason = if before.reset_at != after.reset_at {
        Some("pg_stat_statements was reset during the experiment".into())
    } else if before.deallocations != after.deallocations {
        Some("pg_stat_statements evicted entries during the experiment".into())
    } else if before.track_planning != after.track_planning {
        Some("planning-statistics capability changed between snapshots".into())
    } else if before
        .statements
        .iter()
        .any(|(key, b)| after.statements.get(key).is_some_and(|a| !monotonic(b, a)))
        || before
            .statements
            .keys()
            .any(|key| !after.statements.contains_key(key))
    {
        Some("statement counters decreased or an entry disappeared between snapshots".into())
    } else {
        None
    };
    let valid = reason.is_none();
    let mut entries = Vec::new();
    if valid {
        for (key, after_value) in &after.statements {
            let before_value = before.statements.get(key);
            entries.push(StatementDeltaEntry {
                key: key.clone(),
                delta: diff_counters(before_value, after_value),
            });
        }
    }
    StatementStatsDelta {
        valid,
        reason,
        track_planning: after.track_planning,
        entries,
        limitation: "Statistics are grouped by PostgreSQL database, user, query ID, and top-level status. Query text is omitted. Cumulative counters are deltas; mean/min/max/deviation fields are final snapshot values, not per-run latency distributions. These counters can include other sessions using the workload role, and PostgreSQL may publish in-progress statistics with delay.".into(),
    }
}

fn monotonic(a: &StatementCounters, b: &StatementCounters) -> bool {
    b.calls >= a.calls
        && b.plans >= a.plans
        && b.total_plan_time_ms >= a.total_plan_time_ms
        && b.total_exec_time_ms >= a.total_exec_time_ms
        && b.rows >= a.rows
        && b.shared_blks_hit >= a.shared_blks_hit
        && b.shared_blks_read >= a.shared_blks_read
        && b.shared_blks_dirtied >= a.shared_blks_dirtied
        && b.shared_blks_written >= a.shared_blks_written
        && b.temp_blks_read >= a.temp_blks_read
        && b.temp_blks_written >= a.temp_blks_written
        && b.wal_bytes.parse::<u128>().unwrap_or_default()
            >= a.wal_bytes.parse::<u128>().unwrap_or_default()
}

fn diff_counters(
    before: Option<&StatementCounters>,
    after: &StatementCounters,
) -> StatementCounters {
    let b = before.cloned().unwrap_or(StatementCounters {
        calls: 0,
        plans: 0,
        total_plan_time_ms: 0.0,
        mean_plan_time_ms: 0.0,
        min_plan_time_ms: 0.0,
        max_plan_time_ms: 0.0,
        stddev_plan_time_ms: 0.0,
        total_exec_time_ms: 0.0,
        mean_exec_time_ms: 0.0,
        min_exec_time_ms: 0.0,
        max_exec_time_ms: 0.0,
        stddev_exec_time_ms: 0.0,
        rows: 0,
        shared_blks_hit: 0,
        shared_blks_read: 0,
        shared_blks_dirtied: 0,
        shared_blks_written: 0,
        temp_blks_read: 0,
        temp_blks_written: 0,
        wal_bytes: "0".into(),
    });
    StatementCounters {
        calls: after.calls - b.calls,
        plans: after.plans - b.plans,
        total_plan_time_ms: after.total_plan_time_ms - b.total_plan_time_ms,
        mean_plan_time_ms: after.mean_plan_time_ms,
        min_plan_time_ms: after.min_plan_time_ms,
        max_plan_time_ms: after.max_plan_time_ms,
        stddev_plan_time_ms: after.stddev_plan_time_ms,
        total_exec_time_ms: after.total_exec_time_ms - b.total_exec_time_ms,
        mean_exec_time_ms: after.mean_exec_time_ms,
        min_exec_time_ms: after.min_exec_time_ms,
        max_exec_time_ms: after.max_exec_time_ms,
        stddev_exec_time_ms: after.stddev_exec_time_ms,
        rows: after.rows - b.rows,
        shared_blks_hit: after.shared_blks_hit - b.shared_blks_hit,
        shared_blks_read: after.shared_blks_read - b.shared_blks_read,
        shared_blks_dirtied: after.shared_blks_dirtied - b.shared_blks_dirtied,
        shared_blks_written: after.shared_blks_written - b.shared_blks_written,
        temp_blks_read: after.temp_blks_read - b.temp_blks_read,
        temp_blks_written: after.temp_blks_written - b.temp_blks_written,
        wal_bytes: after
            .wal_bytes
            .parse::<u128>()
            .unwrap_or_default()
            .saturating_sub(b.wal_bytes.parse::<u128>().unwrap_or_default())
            .to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counters(calls: i64) -> StatementCounters {
        StatementCounters {
            calls,
            plans: calls,
            total_plan_time_ms: calls as f64,
            mean_plan_time_ms: 1.0,
            min_plan_time_ms: 1.0,
            max_plan_time_ms: 1.0,
            stddev_plan_time_ms: 0.0,
            total_exec_time_ms: calls as f64 * 2.0,
            mean_exec_time_ms: 2.0,
            min_exec_time_ms: 2.0,
            max_exec_time_ms: 2.0,
            stddev_exec_time_ms: 0.0,
            rows: calls,
            shared_blks_hit: calls,
            shared_blks_read: 0,
            shared_blks_dirtied: 0,
            shared_blks_written: 0,
            temp_blks_read: 0,
            temp_blks_written: 0,
            wal_bytes: (calls * 10).to_string(),
        }
    }

    fn snapshot(calls: i64, deallocations: i64) -> StatementSnapshot {
        let key = StatementKey {
            dbid: 1,
            userid: 2,
            queryid: Some(3),
            toplevel: true,
        };
        StatementSnapshot {
            reset_at: Some(Utc::now()),
            deallocations,
            track_planning: Some(false),
            statements: BTreeMap::from([(key, counters(calls))]),
        }
    }

    #[test]
    fn valid_snapshot_delta_uses_query_identity_and_counter_differences() {
        let before = snapshot(4, 0);
        let mut after = snapshot(9, 0);
        after.reset_at = before.reset_at;
        let delta = delta(&before, &after);
        assert!(delta.valid);
        assert_eq!(delta.entries[0].delta.calls, 5);
        assert_eq!(delta.entries[0].delta.total_exec_time_ms, 10.0);
    }

    #[test]
    fn evicted_or_reset_statistics_are_invalidated() {
        let before = snapshot(4, 0);
        let mut after = snapshot(9, 1);
        after.reset_at = before.reset_at;
        assert!(!delta(&before, &after).valid);
        let mut reset = snapshot(9, 0);
        reset.reset_at = Some(before.reset_at.unwrap() + chrono::Duration::seconds(1));
        assert!(!delta(&before, &reset).valid);
    }
}

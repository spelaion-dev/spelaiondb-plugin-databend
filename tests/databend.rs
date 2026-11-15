//! Tests against a real Databend (1.2.881, its HTTP query handler).
//!
//! They need `SPELAION_TEST_DATABEND=host:port` and, for TLS, `SPELAION_TEST_DATABEND_TLS`
//! (HTTPS with a certificate for `databend.spelaion.test` signed by `testing/keys/ca.crt`). User
//! `spelaion`, password `spelaion-test`. Locally: `ci/services.sh` and
//! `SPELAION_TEST_DATABEND=127.0.0.1:18310 SPELAION_TEST_DATABEND_TLS=127.0.0.1:18443 cargo test`.
//! Without the variables they are skipped with a notice, which does not count as tested.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, LogicalType,
    ObjectKind, ResultSchema, Secret, TlsMode, TransactionState, Value,
};
use spelaion_driver_databend::DatabendDriver;

fn addr(variable: &str) -> Option<(String, u16)> {
    let Ok(addr) = std::env::var(variable) else {
        eprintln!("{variable} not set: test skipped");
        return None;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    Some((host.to_string(), port.parse().expect("a port")))
}

fn params(host: &str, port: u16, database: &str) -> ConnectParams {
    ConnectParams {
        host: host.into(),
        port,
        user: "spelaion".into(),
        password: Some(Secret::new("spelaion-test")),
        database: database.into(),
        tls: TlsMode::Disable,
        ..Default::default()
    }
}

async fn connect(database: &str) -> Option<Box<dyn Connection>> {
    let (host, port) = addr("SPELAION_TEST_DATABEND")?;
    Some(
        DatabendDriver
            .connect(&params(&host, port, database))
            .await
            .expect("connects"),
    )
}

/// A name no other test run uses.
fn unique(prefix: &str) -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    format!(
        "{prefix}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    )
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

async fn exec(conn: &dyn Connection, sql: &str) -> Option<u64> {
    let events = run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
    events.iter().find_map(|e| match e {
        ExecEvent::Done { rows_affected } => *rows_affected,
        _ => None,
    })
}

async fn query(conn: &dyn Connection, sql: &str) -> (ResultSchema, Vec<Vec<Value>>) {
    let events = run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
    let mut schema: Option<ResultSchema> = None;
    let mut rows = Vec::new();
    for event in events {
        match event {
            ExecEvent::Columns(s) => schema = Some((*s).clone()),
            ExecEvent::Batch(batch) => {
                let columns = &schema.as_ref().expect("columns come first").columns;
                for r in 0..batch.num_rows() {
                    rows.push(
                        (0..batch.num_columns())
                            .map(|c| {
                                Value::from_array(batch.column(c), r, columns[c].logical)
                                    .expect("a cell")
                            })
                            .collect(),
                    );
                }
            }
            ExecEvent::Notice(_) | ExecEvent::Done { .. } => {}
        }
    }
    (schema.unwrap_or_else(|| panic!("{sql}: no result")), rows)
}

/// Every kind of value: its logical type, its native type and the cell as the core shows it.
#[tokio::test]
async fn every_type_reads_as_the_server_writes_it() {
    let Some(conn) = connect("").await else {
        return;
    };
    let (schema, rows) = query(
        conn.as_ref(),
        "SELECT true AS b, -7::int8 AS i8, 2147483647::int32 AS i32, \
         9223372036854775807::int64 AS i64, 4294967295::uint32 AS u32, \
         18446744073709551615::uint64 AS u64, 1.5::float32 AS f32, 0.1::float64 AS f64, \
         'NaN'::float64 AS nan, 'inf'::float64 AS inf, 12345.67::decimal(10, 2) AS dec, \
         'texto ñ ''q''' AS s, to_binary('ab') AS bin, '2024-02-29'::date AS d, \
         '2024-02-29 10:20:30.123456'::timestamp AS ts, [1, 2] AS arr, \
         parse_json('{\"a\":[1,2]}') AS var, (1, 'x') AS tup, {'k': 1} AS m, \
         null AS z",
    )
    .await;
    let cells: Vec<(String, LogicalType, String, String)> = schema
        .columns
        .iter()
        .zip(&rows[0])
        .map(|(c, v)| {
            (
                c.name.clone(),
                c.logical,
                c.native_type.clone(),
                v.to_string(),
            )
        })
        .collect();
    let expected: Vec<(&str, LogicalType, &str, &str)> = vec![
        ("b", LogicalType::Bool, "Boolean", "true"),
        ("i8", LogicalType::Int, "Int8", "-7"),
        ("i32", LogicalType::Int, "Int32", "2147483647"),
        ("i64", LogicalType::Int, "Int64", "9223372036854775807"),
        ("u32", LogicalType::Int, "UInt32", "4294967295"),
        (
            "u64",
            LogicalType::Decimal,
            "UInt64",
            "18446744073709551615",
        ),
        ("f32", LogicalType::Float, "Float32", "1.5"),
        ("f64", LogicalType::Float, "Float64", "0.1"),
        ("nan", LogicalType::Float, "Float64", "NaN"),
        ("inf", LogicalType::Float, "Float64", "Infinity"),
        ("dec", LogicalType::Decimal, "Decimal(10, 2)", "12345.67"),
        ("s", LogicalType::Text, "String", "texto ñ 'q'"),
        ("bin", LogicalType::Bytes, "Binary", "\\x6162"),
        ("d", LogicalType::Date, "Date", "2024-02-29"),
        (
            "ts",
            LogicalType::Timestamp,
            "Timestamp",
            "2024-02-29 10:20:30.123456",
        ),
        ("arr", LogicalType::Array, "Array(UInt8)", "[1,2]"),
        ("var", LogicalType::Json, "Variant", "{\"a\":[1,2]}"),
        (
            "tup",
            LogicalType::Native,
            "Tuple(UInt8, String)",
            "(1,\"x\")",
        ),
        ("m", LogicalType::Native, "Map(String, UInt8)", "{\"k\":1}"),
        ("z", LogicalType::Text, "NULL", "NULL"),
    ];
    let expected: Vec<(String, LogicalType, String, String)> = expected
        .into_iter()
        .map(|(n, l, t, v)| (n.into(), l, t.into(), v.into()))
        .collect();
    for (got, want) in cells.iter().zip(&expected) {
        assert_eq!(got, want);
    }
    assert_eq!(cells.len(), expected.len());
}

/// What a statement sets holds for the next ones: the database, and a transaction, whose state
/// comes from the server's answers.
#[tokio::test]
async fn the_session_keeps_the_database_and_the_transaction() {
    let Some(conn) = connect("").await else {
        return;
    };
    let conn = conn.as_ref();
    let db = unique("sdb351");
    exec(conn, &format!("CREATE DATABASE {db}")).await;
    exec(conn, &format!("USE {db}")).await;
    assert_eq!(conn.current_database().await.unwrap(), Some(db.clone()));
    exec(conn, "CREATE TABLE t (id INT, s STRING)").await;
    assert_eq!(
        exec(conn, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").await,
        Some(2)
    );
    assert_eq!(
        exec(conn, "UPDATE t SET s = 'c' WHERE id = 2").await,
        Some(1)
    );

    exec(conn, "BEGIN").await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Open
    );
    exec(conn, "INSERT INTO t VALUES (3, 'tx')").await;
    exec(conn, "ROLLBACK").await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Idle
    );
    let (_, rows) = query(conn, "SELECT count(*) FROM t").await;
    assert_eq!(rows[0][0].to_string(), "2");

    exec(conn, "BEGIN").await;
    exec(conn, "INSERT INTO t VALUES (4, 'kept')").await;
    exec(conn, "COMMIT").await;
    let (_, rows) = query(conn, "SELECT count(*) FROM t").await;
    assert_eq!(rows[0][0].to_string(), "3");

    // A failed statement leaves the transaction failed, as the server says.
    exec(conn, "BEGIN").await;
    assert!(run(conn, "SELECT * FROM not_there").await.is_err());
    assert!(run(conn, "SELECT 1").await.is_err());
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Failed
    );
    exec(conn, "ROLLBACK").await;
    assert_eq!(
        conn.transaction_state().await.unwrap(),
        TransactionState::Idle
    );

    exec(conn, &format!("DROP DATABASE {db}")).await;
}

#[tokio::test]
async fn a_large_result_comes_in_batches_of_the_size_asked() {
    let Some(conn) = connect("").await else {
        return;
    };
    let events: Vec<ExecEvent> = conn
        .execute(
            "SELECT number FROM numbers(25000)",
            &ExecOptions {
                batch_size: 4000,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let sizes: Vec<usize> = events
        .iter()
        .filter_map(|e| match e {
            ExecEvent::Batch(b) => Some(b.num_rows()),
            _ => None,
        })
        .collect();
    assert_eq!(sizes.iter().sum::<usize>(), 25_000);
    assert!(sizes.iter().all(|&n| n <= 4000), "{sizes:?}");
}

#[tokio::test]
async fn a_cancel_stops_the_query_on_the_server() {
    let Some(conn) = connect("").await else {
        return;
    };
    let cancel = conn.cancel_handle();
    let started = Instant::now();
    // A million squared rows to join: minutes, unless it is cancelled.
    let slow = format!(
        "SELECT count(*), '{}' FROM numbers(1000000) a, numbers(1000000) b \
         WHERE a.number + b.number = 7",
        unique("slow")
    );
    let (result, ()) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(30), run(conn.as_ref(), &slow)),
        async {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            cancel.cancel().await.unwrap();
        }
    );
    let result = result.expect("the cancel was obeyed");
    assert!(matches!(result, Err(DriverError::Cancelled)), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(20));
    // Nothing sent after the cancel is touched, and the query is not running any more.
    let marker = slow.split('\'').nth(1).unwrap().to_string();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, rows) = query(
            conn.as_ref(),
            &format!(
                "SELECT count(*) FROM system.processes WHERE extra_info LIKE '%{marker}%' \
                 AND extra_info NOT LIKE '%system.processes%'"
            ),
        )
        .await;
        if rows[0][0].to_string() == "0" {
            break;
        }
        assert!(Instant::now() < deadline, "the query still runs");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test]
async fn refusals_carry_the_servers_code_and_the_session_goes_on() {
    let Some(conn) = connect("").await else {
        return;
    };
    match run(conn.as_ref(), "SELECT * FROM a_table_that_is_not_there").await {
        Err(DriverError::Database { code, message, .. }) => {
            assert!(code.is_some(), "{message}");
            assert!(message.contains("a_table_that_is_not_there"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    let (_, rows) = query(conn.as_ref(), "SELECT 'still here'").await;
    assert_eq!(rows[0][0].to_string(), "still here");
}

#[tokio::test]
async fn the_explorer_reads_databases_tables_and_views() {
    let Some(conn) = connect("").await else {
        return;
    };
    let conn = conn.as_ref();
    let db = unique("sdb351_x");
    exec(conn, &format!("CREATE DATABASE {db}")).await;
    exec(
        conn,
        &format!("CREATE TABLE {db}.parent (id INT NOT NULL, name VARCHAR NULL DEFAULT 'x')"),
    )
    .await;
    exec(
        conn,
        &format!("CREATE VIEW {db}.v AS SELECT id FROM {db}.parent"),
    )
    .await;

    let schemas = conn.list_schemas().await.unwrap();
    assert!(
        schemas.iter().any(|s| s.name == db && !s.system),
        "{schemas:?}"
    );
    assert!(schemas.iter().any(|s| s.name == "system" && s.system));
    assert!(conn.list_databases().await.unwrap().contains(&db));

    let objects = conn.list_objects(&db).await.unwrap();
    let kinds: Vec<(String, ObjectKind)> =
        objects.iter().map(|o| (o.name.clone(), o.kind)).collect();
    assert_eq!(
        kinds,
        [
            ("parent".to_string(), ObjectKind::Table),
            ("v".to_string(), ObjectKind::View)
        ]
    );
    let table = conn.describe_table(&db, "parent").await.unwrap();
    let columns: Vec<(&str, bool)> = table
        .columns
        .iter()
        .map(|c| (c.name.as_str(), c.nullable))
        .collect();
    assert_eq!(columns, [("id", false), ("name", true)]);
    assert!(table.primary_key.is_empty());
    let all = conn.list_columns(&db).await.unwrap();
    assert!(all.iter().any(|c| c.table == "parent" && c.name == "name"));
    let view = conn.relation_definition(&db, "v").await.unwrap();
    assert!(view.starts_with("CREATE VIEW"), "{view}");
    let created = conn.relation_definition(&db, "parent").await.unwrap();
    assert!(created.starts_with("CREATE TABLE"), "{created}");
    assert!(conn.describe_table(&db, "nothing").await.is_err());

    exec(conn, &format!("DROP DATABASE {db}")).await;
}

/// A database name is a quoted identifier: one with a backtick or a quote is a name, never SQL.
#[tokio::test]
async fn names_with_quotes_stay_names() {
    let Some(conn) = connect("").await else {
        return;
    };
    let conn = conn.as_ref();
    assert!(
        conn.select_database("x\"; DROP DATABASE default; --")
            .await
            .is_err()
    );
    // A database whose name holds a backtick, a quote and a backslash is selected and browsed
    // by that name.
    let odd = format!("{}_a`b\"c\\d", unique("sdb351"));
    let quoted = format!("\"{}\"", odd.replace('"', "\"\""));
    exec(conn, &format!("CREATE DATABASE {quoted}")).await;
    exec(conn, &format!("CREATE TABLE {quoted}.t (id INT)")).await;
    conn.select_database(&odd).await.unwrap();
    assert_eq!(conn.current_database().await.unwrap(), Some(odd.clone()));
    let objects = conn.list_objects(&odd).await.unwrap();
    assert_eq!(objects.len(), 1, "{objects:?}");
    assert!(conn.relation_definition(&odd, "t").await.is_ok());
    conn.select_database("default").await.unwrap();
    exec(conn, &format!("DROP DATABASE {quoted}")).await;
    assert!(conn.list_objects("x' OR '1'='1").await.unwrap().is_empty());
    assert!(
        conn.list_objects("x\\' OR 1=1 -- ")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        conn.list_databases()
            .await
            .unwrap()
            .contains(&"default".to_string())
    );
}

#[tokio::test]
async fn connecting_to_the_address_given_keeps_the_host_name() {
    let Some((_, port)) = addr("SPELAION_TEST_DATABEND") else {
        return;
    };
    let mut p = params("databend.invalid", 1, "");
    p.connect_addr = Some(format!("127.0.0.1:{port}").parse().unwrap());
    let conn = DatabendDriver.connect(&p).await.unwrap();
    let (_, rows) = query(conn.as_ref(), "SELECT 1").await;
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn a_database_that_is_not_there_is_a_connection_error() {
    let Some((host, port)) = addr("SPELAION_TEST_DATABEND") else {
        return;
    };
    match DatabendDriver
        .connect(&params(&host, port, "nowhere_to_be_found"))
        .await
    {
        Err(DriverError::Connection(_)) => {}
        Err(other) => panic!("{other:?}"),
        Ok(_) => panic!("connected to a database that is not there"),
    }
}

#[tokio::test]
async fn a_wrong_password_is_refused_and_never_shown() {
    let Some((host, port)) = addr("SPELAION_TEST_DATABEND") else {
        return;
    };
    let mut p = params(&host, port, "");
    p.password = Some(Secret::new("not-the-Password-42"));
    let error = DatabendDriver.connect(&p).await.err().expect("refused");
    let text = error.to_string();
    assert!(text.contains("refused") || text.contains("401"), "{text}");
    assert!(!text.contains("not-the-Password-42"), "{text}");
    // Nor the header that carries it.
    assert!(!text.contains("c3BlbGFpb246bm90"), "{text}");
}

fn keys() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testing/keys")
}

/// TLS to 127.0.0.1, the address a tunnel gives, with the certificate checked against the
/// connection's host name: that name works, any other is refused.
#[tokio::test]
async fn tls_is_verified_against_the_host_name_not_the_address() {
    let Some((host, port)) = addr("SPELAION_TEST_DATABEND_TLS") else {
        return;
    };
    let tls = |name: &str| ConnectParams {
        connect_addr: Some(format!("{host}:{port}").parse().unwrap()),
        tls: TlsMode::VerifyFull,
        ca_certificate: Some(keys().join("ca.crt")),
        ..params(name, port, "")
    };
    let conn = DatabendDriver
        .connect(&tls("databend.spelaion.test"))
        .await
        .expect("verify-full against the name");
    let (_, rows) = query(conn.as_ref(), "SELECT 'over tls'").await;
    assert_eq!(rows[0][0].to_string(), "over tls");
    let refused = DatabendDriver
        .connect(&tls("another.spelaion.test"))
        .await
        .err()
        .expect("another name is refused");
    assert!(refused.to_string().contains("TLS"), "{refused}");
}

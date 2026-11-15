//! Databend for SpelaionDB, served as a plugin (SpelaionDB's ADR-041 and ADR-042).
//!
//! Databend's HTTP query API: a statement is posted to `/v1/query/` with the session, then its
//! results are read through each answer's `next_uri`, a page at a time, until there is none (the
//! last one releases the query on the server). A `GET` on its `kill_uri` cancels it.
//!
//! The session lives in the client: the server's answers carry it (the database, the settings,
//! the open transaction) and it is sent back with every request, so `USE`, `SET` and `BEGIN`
//! hold for the next statements, as in any engine. The connection's requests go one at a time,
//! through an actor.
//!
//! What Databend does not have is declared, and SpelaionDB hides it: editing rows (no primary
//! keys), importing rows, binding values, plans. It enforces no read-only mode (SpelaionDB's
//! SDB-350), so SpelaionDB offers no read-only connection and no agents for it.

pub mod http;
mod introspect;
pub mod tls;
pub mod types;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use hyper::{Method, StatusCode};
use serde_json::{Value as Json, json};
use spelaion_driver_api::{
    CancelHandle, Capabilities, ConnectParams, Connection, Driver, DriverError, EventStream,
    ExecEvent, ExecOptions, KeepAlive, ObjectInfo, ReferencingKey, Result, ResultSchema,
    SchemaColumn, SchemaInfo, TableInfo, TransactionState,
};
use tokio::sync::{mpsc, oneshot, watch};

use crate::http::{Client, Endpoint, path_of};
use crate::tls::ClientIdentity;
use crate::types::Batches;

/// How long one request for a page waits on the server for rows before it answers.
const WAIT_SECS: u64 = 1;

/// The most rows the server puts in one page.
const MAX_PAGE_ROWS: usize = 10_000;

/// The engine.
#[derive(Debug, Clone, Copy, Default)]
pub struct DatabendDriver;

#[async_trait]
impl Driver for DatabendDriver {
    fn engine(&self) -> &str {
        "databend"
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        let identity = match (&params.client_certificate, &params.client_key) {
            (Some(cert), Some(key)) => Some(ClientIdentity { cert, key }),
            (None, None) => None,
            _ => {
                return Err(DriverError::Connection(
                    "a client certificate needs its key, and a key its certificate".into(),
                ));
            }
        };
        let tls = tls::configure(params.tls, params.ca_certificate.as_deref(), identity)?;
        let user = if params.user.is_empty() {
            "root".to_string()
        } else {
            params.user.clone()
        };
        let endpoint = Arc::new(Endpoint::new(
            params.host.clone(),
            params.port,
            params.connect_addr,
            tls,
            params.connect_timeout(),
            &user,
            params.password.clone(),
        ));
        let mut session = Session {
            client: Client::new(endpoint),
            state: None,
        };
        // `USE` fails for a database that does not exist; a session that names one is not
        // checked by the server.
        let check = if params.database.is_empty() {
            "SELECT 1".to_string()
        } else {
            format!("USE {}", quote(&params.database))
        };
        match tokio::time::timeout(params.connect_timeout(), session.rows(&check)).await {
            Ok(Ok(_)) => {}
            Ok(Err(DriverError::Database { message, .. })) => {
                return Err(DriverError::Connection(message));
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(DriverError::Connection(format!(
                    "the server did not answer within {} s",
                    params.connect_timeout().as_secs()
                )));
            }
        }
        let closed = Arc::new(AtomicBool::new(false));
        let (cancel_tx, cancel_rx) = watch::channel(0u64);
        let cancel = Arc::new(Cancel {
            sent: AtomicU64::new(0),
            upto: cancel_tx,
        });
        let (requests, receiver) = mpsc::channel(16);
        tokio::spawn(actor(session, receiver, cancel_rx, closed.clone()));
        Ok(Box::new(DatabendConnection {
            capabilities: Capabilities {
                transactions: true,
                cancel: true,
                explain: false,
                editable_results: false,
                schemas: true,
                insert_rows: false,
                bound_params: false,
            },
            requests,
            cancel,
            closed,
        }))
    }
}

/// `name` as a quoted identifier. Double quotes, with a quote doubled: Databend's backticks
/// cannot hold a backtick at all (neither doubled nor escaped). A backslash inside double
/// quotes is kept as it is.
pub fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `text` as a string literal. Databend reads backslash escapes inside strings, so a backslash
/// is doubled as well as a quote.
pub fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
}

/// The client's side of the session: the server's last word on it, sent back with every
/// request, and the connection the requests go through.
struct Session {
    client: Client,
    /// The `session` object of the last answer, as the server wrote it.
    state: Option<Json>,
}

/// One page of a statement's results.
struct Page {
    columns: Option<Vec<(String, String)>>,
    data: Vec<Vec<Json>>,
    next: Option<String>,
    kill: Option<String>,
    error: Option<DriverError>,
}

/// The one column Databend answers a data change with, and its rows' count.
fn affected_column(columns: &[(String, String)]) -> bool {
    matches!(
        columns,
        [(name, _)] if matches!(
            name.as_str(),
            "number of rows inserted" | "number of rows updated" | "number of rows deleted"
        )
    )
}

impl Session {
    fn field(&self, name: &str) -> Option<&str> {
        self.state.as_ref()?.get(name)?.as_str()
    }

    fn transaction(&self) -> TransactionState {
        match self.field("txn_state") {
            Some("Active") => TransactionState::Open,
            Some("Fail") => TransactionState::Failed,
            _ => TransactionState::Idle,
        }
    }

    fn database(&self) -> Option<String> {
        self.field("database").map(str::to_string)
    }

    async fn read_page(
        &mut self,
        method: Method,
        path: &str,
        body: Option<String>,
    ) -> Result<Page> {
        let response = self.client.send(method, path, &[], body).await?;
        let scrub = |text: &str| self.client.endpoint().scrub(text);
        let json: Option<Json> = serde_json::from_slice(&response.body).ok();
        if response.status != StatusCode::OK {
            let text = scrub(String::from_utf8_lossy(&response.body).trim());
            return Err(match response.status {
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                    DriverError::Connection(format!(
                        "the server refused the user or password ({})",
                        response.status
                    ))
                }
                // A query the server let go (killed, expired): what it says, as an error.
                _ if json.as_ref().and_then(|j| j.get("error")).is_some() => {
                    let error = json.as_ref().and_then(|j| j.get("error")).expect("checked");
                    server_error(error, &scrub)
                }
                _ if text.is_empty() => {
                    DriverError::Connection(format!("the server answered {}", response.status))
                }
                _ => DriverError::Connection(format!(
                    "the server answered {}: {text}",
                    response.status
                )),
            });
        }
        let json =
            json.ok_or_else(|| DriverError::Other("the server's answer is not JSON".into()))?;
        if let Some(state) = json.get("session").filter(|s| s.is_object()) {
            self.state = Some(state.clone());
        }
        let columns = json
            .get("schema")
            .and_then(Json::as_array)
            .filter(|columns| !columns.is_empty())
            .map(|columns| {
                columns
                    .iter()
                    .map(|c| {
                        let text = |key: &str| {
                            c.get(key)
                                .and_then(Json::as_str)
                                .unwrap_or_default()
                                .to_string()
                        };
                        (text("name"), text("type"))
                    })
                    .collect()
            });
        let data = match json.get("data") {
            Some(Json::Array(rows)) => rows
                .iter()
                .map(|row| row.as_array().cloned().unwrap_or_default())
                .collect(),
            _ => Vec::new(),
        };
        let uri = |key: &str| json.get(key).and_then(Json::as_str).map(str::to_string);
        let error = json
            .get("error")
            .filter(|e| !e.is_null())
            .map(|e| server_error(e, &scrub));
        Ok(Page {
            columns,
            data,
            next: uri("next_uri"),
            kill: uri("kill_uri"),
            error,
        })
    }

    fn body(&self, sql: &str, page_rows: usize) -> String {
        let mut body = json!({
            "sql": sql,
            "pagination": {
                "wait_time_secs": WAIT_SECS,
                "max_rows_per_page": page_rows.clamp(1, MAX_PAGE_ROWS),
            },
        });
        if let Some(state) = &self.state {
            body["session"] = state.clone();
        }
        body.to_string()
    }

    /// A whole result: what the driver's own queries read.
    async fn rows(&mut self, sql: &str) -> Result<Vec<Vec<Json>>> {
        let body = self.body(sql, MAX_PAGE_ROWS);
        let mut page = self
            .read_page(Method::POST, "/v1/query/", Some(body))
            .await?;
        let mut rows = Vec::new();
        loop {
            if let Some(error) = page.error.take() {
                self.release(page.next.take()).await;
                return Err(error);
            }
            rows.append(&mut page.data);
            let Some(next) = page.next.take() else {
                return Ok(rows);
            };
            let path = path_of(&next).ok_or_else(|| bad_uri(&next))?;
            page = self.read_page(Method::GET, &path, None).await?;
        }
    }

    /// Follows a finished query's `next_uri` to its end, which releases it on the server.
    async fn release(&mut self, next: Option<String>) {
        let mut next = next;
        for _ in 0..4 {
            let Some(path) = next.as_deref().and_then(path_of) else {
                return;
            };
            next = match self.read_page(Method::GET, &path, None).await {
                Ok(page) => page.next,
                Err(_) => return,
            };
        }
    }
}

fn bad_uri(uri: &str) -> DriverError {
    DriverError::Other(format!(
        "the server sent an address that cannot be followed: {uri}"
    ))
}

/// The server's error (`{"code": 1005, "message": "…"}`), as a driver error.
fn server_error(error: &Json, scrub: &dyn Fn(&str) -> String) -> DriverError {
    let code = match error.get("code") {
        Some(Json::Number(n)) => Some(n.to_string()),
        Some(Json::String(s)) => Some(s.clone()),
        _ => None,
    };
    let message = error
        .get("message")
        .and_then(Json::as_str)
        .unwrap_or("the server reported an error")
        .trim();
    DriverError::Database {
        code,
        message: scrub(message),
        detail: None,
        hint: None,
        position: None,
    }
}

/// Cancels: remembers the last statement sent when it was asked, so that statement and every
/// one queued before it stop (a queued one never starts), and none sent after it is touched.
struct Cancel {
    sent: AtomicU64,
    upto: watch::Sender<u64>,
}

struct DatabendCancel(Arc<Cancel>);

#[async_trait]
impl CancelHandle for DatabendCancel {
    async fn cancel(&self) -> Result<()> {
        let sent = self.0.sent.load(Ordering::SeqCst);
        self.0.upto.send_if_modified(|upto| {
            if *upto < sent {
                *upto = sent;
                true
            } else {
                false
            }
        });
        Ok(())
    }
}

enum Request {
    Execute {
        sql: String,
        batch_size: usize,
        seq: u64,
        events: mpsc::Sender<Result<ExecEvent>>,
        started: oneshot::Sender<Result<()>>,
    },
    Rows {
        sql: String,
        reply: oneshot::Sender<Result<Vec<Vec<Json>>>>,
    },
    State {
        reply: oneshot::Sender<(TransactionState, Option<String>)>,
    },
}

async fn actor(
    mut session: Session,
    mut requests: mpsc::Receiver<Request>,
    cancel: watch::Receiver<u64>,
    closed: Arc<AtomicBool>,
) {
    while let Some(request) = requests.recv().await {
        match request {
            Request::Execute {
                sql,
                batch_size,
                seq,
                events,
                started,
            } => {
                execute(
                    &mut session,
                    cancel.clone(),
                    seq,
                    &sql,
                    batch_size,
                    events,
                    started,
                    &closed,
                )
                .await;
            }
            Request::Rows { sql, reply } => {
                let result = session.rows(&sql).await;
                if let Err(DriverError::Connection(_)) = &result {
                    closed.store(true, Ordering::SeqCst);
                }
                let _ = reply.send(result);
            }
            Request::State { reply } => {
                let _ = reply.send((session.transaction(), session.database()));
            }
        }
    }
}

/// Asks the server to kill the query at `kill`, on a connection of its own. The query is then
/// closed on the server: nothing more of it is read.
async fn kill_on_server(session: &Session, kill: Option<&str>) {
    let Some(path) = kill.and_then(path_of) else {
        return;
    };
    let mut alone = Client::new(session.client.endpoint().clone());
    let _ = tokio::time::timeout(
        Duration::from_secs(10),
        alone.send(Method::GET, &path, &[], None),
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn execute(
    session: &mut Session,
    mut cancel: watch::Receiver<u64>,
    seq: u64,
    sql: &str,
    batch_size: usize,
    events: mpsc::Sender<Result<ExecEvent>>,
    started: oneshot::Sender<Result<()>>,
    closed: &AtomicBool,
) {
    if *cancel.borrow() >= seq {
        let _ = started.send(Err(DriverError::Cancelled));
        return;
    }
    let batch_size = batch_size.max(1);
    let body = session.body(sql, batch_size);
    let first = session
        .read_page(Method::POST, "/v1/query/", Some(body))
        .await;
    let mut page = match first {
        Ok(mut page) => match page.error.take() {
            Some(error) => {
                session.release(page.next.take()).await;
                let _ = started.send(Err(error));
                return;
            }
            None => page,
        },
        Err(e) => {
            if let DriverError::Connection(_) = e {
                closed.store(true, Ordering::SeqCst);
            }
            let _ = started.send(Err(e));
            return;
        }
    };
    let _ = started.send(Ok(()));

    let mut kill = page.kill.take();
    let mut batches: Option<Batches> = None;
    // A data change answers with one column holding its count: that is rows affected, not a
    // result.
    let mut affected: Option<Option<u64>> = None;
    let outcome: Result<()> = loop {
        if let Some(uri) = page.kill.take() {
            kill = Some(uri);
        }
        if batches.is_none()
            && affected.is_none()
            && let Some(columns) = page.columns.take()
        {
            if affected_column(&columns) {
                affected = Some(None);
            } else {
                let schema = Arc::new(ResultSchema::new(
                    columns
                        .into_iter()
                        .map(|(name, native)| types::column(name, native))
                        .collect(),
                ));
                if events
                    .send(Ok(ExecEvent::Columns(schema.clone())))
                    .await
                    .is_err()
                {
                    kill_on_server(session, kill.as_deref()).await;
                    return;
                }
                batches = Some(Batches::new(schema));
            }
        }
        if let Some(count) = affected.as_mut() {
            if let Some(n) = page
                .data
                .first()
                .and_then(|row| row.first())
                .and_then(Json::as_str)
                .and_then(|n| n.parse::<u64>().ok())
            {
                *count = Some(n);
            }
        } else if let Some(b) = batches.as_mut() {
            let mut failed = None;
            for row in &page.data {
                let full = match b.push(row) {
                    Ok(()) if b.rows() >= batch_size => b.take(),
                    Ok(()) => Ok(None),
                    Err(e) => Err(e),
                };
                match full {
                    Ok(Some(batch)) => {
                        if events.send(Ok(ExecEvent::Batch(batch))).await.is_err() {
                            kill_on_server(session, kill.as_deref()).await;
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        failed = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = failed {
                kill_on_server(session, kill.as_deref()).await;
                break Err(e);
            }
        }
        if let Some(error) = page.error.take() {
            session.release(page.next.take()).await;
            break Err(error);
        }
        let Some(next) = page.next.take() else {
            break Ok(());
        };
        let Some(path) = path_of(&next) else {
            break Err(bad_uri(&next));
        };
        if *cancel.borrow() >= seq {
            kill_on_server(session, kill.as_deref()).await;
            break Err(DriverError::Cancelled);
        }
        let read = {
            let fetch = session.read_page(Method::GET, &path, None);
            tokio::pin!(fetch);
            loop {
                tokio::select! {
                    read = &mut fetch => break Some(read),
                    changed = cancel.changed() => {
                        if changed.is_ok() && *cancel.borrow() >= seq {
                            break None;
                        }
                    }
                }
            }
        };
        match read {
            Some(Ok(next_page)) => page = next_page,
            Some(Err(e)) => break Err(e),
            None => {
                kill_on_server(session, kill.as_deref()).await;
                break Err(DriverError::Cancelled);
            }
        }
    };
    match outcome {
        Ok(()) => {
            if let Some(b) = batches.as_mut() {
                match b.take() {
                    Ok(Some(batch)) => {
                        if events.send(Ok(ExecEvent::Batch(batch))).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        let _ = events.send(Err(e)).await;
                        return;
                    }
                }
            }
            let rows_affected = affected.flatten();
            let _ = events.send(Ok(ExecEvent::Done { rows_affected })).await;
        }
        Err(e) => {
            if let DriverError::Connection(_) = e {
                closed.store(true, Ordering::SeqCst);
            }
            let _ = events.send(Err(e)).await;
        }
    }
}

struct DatabendConnection {
    capabilities: Capabilities,
    requests: mpsc::Sender<Request>,
    cancel: Arc<Cancel>,
    closed: Arc<AtomicBool>,
}

impl DatabendConnection {
    fn gone() -> DriverError {
        DriverError::Connection("the connection is closed".into())
    }

    /// One of the driver's own queries, as JSON rows (each value a string or `null`).
    pub(crate) async fn rows(&self, sql: &str) -> Result<Vec<Vec<Json>>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::gone());
        }
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::Rows {
                sql: sql.to_string(),
                reply,
            })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())?
    }

    async fn state(&self) -> Result<(TransactionState, Option<String>)> {
        let (reply, answer) = oneshot::channel();
        self.requests
            .send(Request::State { reply })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())
    }
}

#[async_trait]
impl Connection for DatabendConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, sql: &str, opts: &ExecOptions) -> Result<EventStream> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(Self::gone());
        }
        let seq = self.cancel.sent.fetch_add(1, Ordering::SeqCst) + 1;
        let (events, receiver) = mpsc::channel(4);
        let (started, answer) = oneshot::channel();
        self.requests
            .send(Request::Execute {
                sql: sql.to_string(),
                batch_size: opts.batch_size,
                seq,
                events,
                started,
            })
            .await
            .map_err(|_| Self::gone())?;
        answer.await.map_err(|_| Self::gone())??;
        let mut receiver = receiver;
        Ok(futures_util::stream::poll_fn(move |cx| receiver.poll_recv(cx)).boxed())
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(DatabendCancel(self.cancel.clone()))
    }

    async fn server_version(&self) -> Result<String> {
        let rows = self.rows("SELECT version()").await?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .and_then(Json::as_str)
            .unwrap_or_default()
            .to_string())
    }

    async fn keep_alive(&self) -> Result<KeepAlive> {
        if self.transaction_state().await? != TransactionState::Idle {
            return Ok(KeepAlive::InTransaction);
        }
        self.rows("SELECT 1").await?;
        Ok(KeepAlive::Alive)
    }

    /// Known without asking: the server's last answer said it.
    async fn transaction_state(&self) -> Result<TransactionState> {
        Ok(self.state().await?.0)
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn current_database(&self) -> Result<Option<String>> {
        Ok(self.state().await?.1)
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        self.rows(&format!("USE {}", quote(database)))
            .await
            .map(|_| ())
    }

    /// Databend's databases are the explorer's schemas, as MySQL's are.
    async fn list_databases(&self) -> Result<Vec<String>> {
        Ok(introspect::list_schemas(self)
            .await?
            .into_iter()
            .map(|s| s.name)
            .collect())
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        introspect::list_schemas(self).await
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        introspect::list_objects(self, schema).await
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        introspect::describe_table(self, schema, name).await
    }

    async fn list_columns(&self, schema: &str) -> Result<Vec<SchemaColumn>> {
        introspect::list_columns(self, schema).await
    }

    /// Databend has no foreign keys.
    async fn list_referencing_keys(
        &self,
        _schema: &str,
        _name: &str,
    ) -> Result<Vec<ReferencingKey>> {
        Ok(Vec::new())
    }

    async fn relation_definition(&self, schema: &str, name: &str) -> Result<String> {
        introspect::relation_definition(self, schema, name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_strings_are_quoted() {
        assert_eq!(quote("a`b"), "\"a`b\"");
        assert_eq!(quote("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote("a\\b"), "\"a\\b\"");
        assert_eq!(literal("it's"), "'it''s'");
        assert_eq!(literal("a\\'; DROP"), "'a\\\\''; DROP'");
    }

    #[test]
    fn errors_keep_their_code() {
        let error: Json =
            serde_json::from_str(r#"{"code":1025,"message":"Unknown table 'x'"}"#).unwrap();
        match server_error(&error, &|t| t.to_string()) {
            DriverError::Database { code, message, .. } => {
                assert_eq!(code.as_deref(), Some("1025"));
                assert_eq!(message, "Unknown table 'x'");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_data_change_is_its_count() {
        let one = |name: &str| vec![(name.to_string(), "UInt64".to_string())];
        assert!(affected_column(&one("number of rows inserted")));
        assert!(affected_column(&one("number of rows deleted")));
        assert!(!affected_column(&one("number")));
        assert!(!affected_column(&[
            ("number of rows inserted".into(), "UInt64".into()),
            ("x".into(), "UInt64".into())
        ]));
    }
}

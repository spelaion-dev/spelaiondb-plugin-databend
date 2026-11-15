//! What the server holds: its databases (the explorer's schemas, as MySQL's are), and in each
//! its tables and views, from `system.databases` and `information_schema`. Names are written as
//! quoted identifiers and string literals (the API binds no values).
//!
//! Databend has no primary keys or foreign keys: none is reported, so nothing pages or edits
//! rows by one.

use serde_json::Value as Json;
use spelaion_driver_api::{
    ColumnInfo, DriverError, ObjectInfo, ObjectKind, Result, SchemaColumn, SchemaInfo, TableInfo,
};

use crate::{DatabendConnection, literal, quote};

fn text(row: &[Json], i: usize) -> String {
    match row.get(i) {
        Some(Json::String(s)) => s.clone(),
        Some(Json::Null) | None => String::new(),
        Some(other) => other.to_string(),
    }
}

fn optional(row: &[Json], i: usize) -> Option<String> {
    Some(text(row, i)).filter(|s| !s.is_empty())
}

fn kind(table_type: &str) -> ObjectKind {
    if table_type == "VIEW" {
        ObjectKind::View
    } else {
        ObjectKind::Table
    }
}

pub async fn list_schemas(conn: &DatabendConnection) -> Result<Vec<SchemaInfo>> {
    let rows = conn
        .rows("SELECT name FROM system.databases ORDER BY name")
        .await?;
    Ok(rows
        .iter()
        .map(|row| {
            let name = text(row, 0);
            SchemaInfo {
                system: matches!(name.as_str(), "system" | "information_schema"),
                name,
            }
        })
        .collect())
}

pub async fn list_objects(conn: &DatabendConnection, schema: &str) -> Result<Vec<ObjectInfo>> {
    let rows = conn
        .rows(&format!(
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_schema = {} ORDER BY table_name",
            literal(schema)
        ))
        .await?;
    Ok(rows
        .iter()
        .map(|row| ObjectInfo {
            schema: schema.to_string(),
            name: text(row, 0),
            kind: kind(&text(row, 1)),
            estimated_rows: None,
            comment: None,
        })
        .collect())
}

pub async fn describe_table(
    conn: &DatabendConnection,
    schema: &str,
    name: &str,
) -> Result<TableInfo> {
    let objects = conn
        .rows(&format!(
            "SELECT table_type FROM information_schema.tables \
             WHERE table_schema = {} AND table_name = {}",
            literal(schema),
            literal(name)
        ))
        .await?;
    let Some(object) = objects.first() else {
        return Err(DriverError::Other(format!(
            "there is no table {schema}.{name}"
        )));
    };
    let columns = conn
        .rows(&format!(
            "SELECT column_name, data_type, is_nullable, column_default \
             FROM information_schema.columns \
             WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
            literal(schema),
            literal(name)
        ))
        .await?;
    Ok(TableInfo {
        object: ObjectInfo {
            schema: schema.to_string(),
            name: name.to_string(),
            kind: kind(&text(object, 0)),
            estimated_rows: None,
            comment: None,
        },
        columns: columns
            .iter()
            .map(|row| ColumnInfo {
                name: text(row, 0),
                native_type: text(row, 1),
                nullable: text(row, 2) == "YES",
                default: optional(row, 3),
                comment: None,
            })
            .collect(),
        primary_key: Vec::new(),
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        constraints: Vec::new(),
    })
}

pub async fn list_columns(conn: &DatabendConnection, schema: &str) -> Result<Vec<SchemaColumn>> {
    let rows = conn
        .rows(&format!(
            "SELECT table_name, column_name, data_type FROM information_schema.columns \
             WHERE table_schema = {} ORDER BY table_name, ordinal_position",
            literal(schema)
        ))
        .await?;
    Ok(rows
        .iter()
        .map(|row| SchemaColumn {
            table: text(row, 0),
            name: text(row, 1),
            native_type: text(row, 2),
        })
        .collect())
}

/// A table's or a view's `CREATE` statement (`SHOW CREATE TABLE` answers both).
pub async fn relation_definition(
    conn: &DatabendConnection,
    schema: &str,
    name: &str,
) -> Result<String> {
    let rows = conn
        .rows(&format!(
            "SHOW CREATE TABLE {}.{}",
            quote(schema),
            quote(name)
        ))
        .await?;
    rows.first()
        .and_then(|row| optional(row, 1))
        .map(|sql| spelaion_driver_api::terminated(&sql))
        .ok_or_else(|| DriverError::Other(format!("no definition for {schema}.{name}")))
}

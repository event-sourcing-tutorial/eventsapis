use crate::pgpool::PgPool;
use anyhow::anyhow;
use anystruct::IntoProto;
use async_trait::async_trait;
use eventsapis_proto::{PollCommandsResponse, PollEventsResponse};
use log::debug;
use serde_json::Value;
use std::{fmt::Debug, str::from_utf8, time::SystemTime};
use time::{format_description::well_known::Iso8601, OffsetDateTime};
use tokio_postgres::{
    types::{FromSql, Type},
    Row,
};
use uuid::Uuid;

#[async_trait]
pub trait Message<R>: Send + Sync + Debug + 'static {
    fn from_row(row: Row) -> Self;
    fn get_idx(&self) -> i64;
    async fn from_str(payload: &str, pool: &PgPool) -> Self;
    fn select_query() -> &'static str;
    fn listen_query() -> &'static str;
    fn into_response(&self) -> R;
}

#[derive(Debug)]
pub struct EventMessage {
    pub idx: i64,
    pub inserted: SystemTime,
    pub payload: Value,
}

#[async_trait]
impl Message<PollEventsResponse> for EventMessage {
    fn from_row(row: Row) -> Self {
        EventMessage {
            idx: row.get(0),
            inserted: row.get(1),
            payload: row.get(2),
        }
    }
    fn get_idx(&self) -> i64 {
        self.idx
    }
    async fn from_str(payload: &str, pool: &PgPool) -> Self {
        parse_message(payload, pool).await
    }
    fn select_query() -> &'static str {
        "select idx, inserted, payload from events where idx > $1 order by idx"
    }
    fn listen_query() -> &'static str {
        "listen event"
    }
    fn into_response(&self) -> PollEventsResponse {
        PollEventsResponse {
            idx: self.idx,
            inserted: Some(self.inserted.into()),
            payload: Some(self.payload.clone().into_proto()),
        }
    }
}

fn parse_unsigned_i64(payload: &[u8], i: usize) -> (i64, usize) {
    let mut n: i64 = 0;
    let mut i = i;
    while i < payload.len() && payload[i] != b',' {
        assert!(payload[i] >= b'0' && payload[i] <= b'9');
        n = n * 10 + (payload[i] - b'0') as i64;
        i += 1;
    }
    (n, i)
}

fn parse_noncomma(bytes: &[u8], i: usize) -> (String, usize) {
    let mut j = i;
    while j < bytes.len() && bytes[j] != b',' {
        j += 1;
    }
    (from_utf8(&bytes[i..j]).unwrap().to_string(), j)
}

fn parse_comma(bytes: &[u8], i: usize) -> usize {
    assert!(i < bytes.len());
    assert!(bytes[i] == b',');
    i + 1
}

fn parse_rest_json(bytes: &[u8], i: usize) -> Value {
    serde_json::from_slice(&bytes[i..bytes.len()]).unwrap()
}

pub async fn parse_message(message: &str, pool: &PgPool) -> EventMessage {
    debug!("parsing message '{}'", message);
    let message = message.replace("\n", ""); // may be a bug in the postgres driver
    let bytes = message.as_bytes();
    let i = 0;
    let (idx, i) = parse_unsigned_i64(bytes, i);
    let i = parse_comma(bytes, i);
    let (inserted, i) = parse_noncomma(bytes, i);
    let payload = if i == bytes.len() {
        pool.get_payload(idx).await
    } else {
        let i = parse_comma(bytes, i);
        parse_rest_json(bytes, i)
    };
    let inserted = format!("{}Z", inserted);
    let inserted: OffsetDateTime = OffsetDateTime::parse(&inserted, &Iso8601::DEFAULT).unwrap();
    let inserted: SystemTime = inserted.into();
    EventMessage {
        idx,
        inserted,
        payload,
    }
}

#[derive(Debug)]
pub enum CommandStatus {
    Issued,
    Finalized,
}

impl<'a> FromSql<'a> for CommandStatus {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match raw {
            b"issued" => Ok(CommandStatus::Issued),
            b"finalized" => Ok(CommandStatus::Finalized),
            _ => Err(anyhow!("invalid final_status {:?}", raw).into()),
        }
    }
    fn accepts(ty: &Type) -> bool {
        ty.name() == "command_status"
    }
}

#[derive(Debug)]
pub enum CommandFinalStatus {
    Succeeded,
    Failed,
    Aborted,
}

impl<'a> FromSql<'a> for CommandFinalStatus {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match raw {
            b"succeeded" => Ok(CommandFinalStatus::Succeeded),
            b"failed" => Ok(CommandFinalStatus::Failed),
            b"aborted" => Ok(CommandFinalStatus::Aborted),
            _ => Err(anyhow!("invalid final_status {:?}", raw).into()),
        }
    }
    fn accepts(ty: &Type) -> bool {
        ty.name() == "final_status"
    }
}

#[derive(Debug)]
pub struct QueueMessage {
    pub idx: i64,
    pub command_id: Uuid,
    pub status: CommandStatus,
    pub updated: SystemTime,
}

#[async_trait]
impl Message<PollCommandsResponse> for QueueMessage {
    fn get_idx(&self) -> i64 {
        self.idx
    }

    fn listen_query() -> &'static str {
        "listen command"
    }

    fn select_query() -> &'static str {
        "select idx, command_id, status, updated from command_stream where idx > $1 order by idx"
    }

    fn from_row(row: Row) -> Self {
        Self {
            idx: row.get("idx"),
            command_id: row.get("command_id"),
            status: row.get("status"),
            updated: row.get("updated"),
        }
    }

    async fn from_str(_payload: &str, _pool: &PgPool) -> Self {
        todo!();
    }

    fn into_response(&self) -> PollCommandsResponse {
        PollCommandsResponse {
            idx: self.idx,
            command_id: self.command_id.to_string(),
            status: match self.status {
                CommandStatus::Issued => eventsapis_proto::CommandStatus::Issued.into(),
                CommandStatus::Finalized => eventsapis_proto::CommandStatus::Finalized.into(),
            },
            updated: Some(self.updated.into()),
        }
    }
}

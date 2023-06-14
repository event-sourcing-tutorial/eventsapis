use log::debug;
use serde_json::Value;
use std::{str::from_utf8, time::SystemTime};
use time::{format_description::well_known::Iso8601, OffsetDateTime};

#[derive(Debug)]
pub struct Message {
    pub idx: i64,
    pub inserted: SystemTime,
    pub payload: Value,
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

pub async fn parse_message(message: &str) -> (i64, SystemTime, Option<Value>) {
    debug!("parsing message '{}'", message);
    let message = message.replace("\n", ""); // may be a bug in the postgres driver
    let bytes = message.as_bytes();
    let i = 0;
    let (idx, i) = parse_unsigned_i64(bytes, i);
    let i = parse_comma(bytes, i);
    let (inserted, i) = parse_noncomma(bytes, i);
    let payload = if i == bytes.len() {
        None
    } else {
        let i = parse_comma(bytes, i);
        Some(parse_rest_json(bytes, i))
    };
    let inserted = format!("{}Z", inserted);
    let inserted: OffsetDateTime = OffsetDateTime::parse(&inserted, &Iso8601::DEFAULT).unwrap();
    let inserted: SystemTime = inserted.into();
    (idx, inserted, payload)
}

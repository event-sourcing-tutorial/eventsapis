use anystruct::{IntoJSON, IntoProto};
use clap::{Parser, Subcommand};
use eventsapis_proto::{
    events_apis_client::EventsApisClient, GetEventRequest, GetEventResponse, GetLastIdxRequest,
    GetLastIdxResponse, InsertEventRequest, InsertEventResponse, PollEventsRequest,
    PollEventsResponse,
};
use std::process::exit;
use time::{format_description::well_known::Rfc3339, Duration, OffsetDateTime};
use tonic::Request;

#[derive(Debug, Parser)]
#[command(name = "cli")]
#[command(about = "Command Line Interface to Events Apis", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// returns the last event index, or 0 if no events were inserted
    #[command(arg_required_else_help = false)]
    Last {},
    /// Gets a specific event
    #[command(arg_required_else_help = true)]
    Get {
        /// the event index to get
        #[arg(value_name = "idx")]
        idx: i64,
    },
    /// Inserts a specific event at an index
    #[command(arg_required_else_help = true)]
    Put {
        /// the event index to insert
        #[arg(value_name = "idx")]
        idx: i64,
        /// the event data to insert, must be proper JSON
        #[arg(value_name = "payload")]
        payload: String,
    },
    #[command(arg_required_else_help = true)]
    Tail {
        /// last event seen, or, number of events to skip
        #[arg(value_name = "idx")]
        idx: i64,
    },
}

fn print_message(idx: i64, inserted: prost_types::Timestamp, payload: prost_types::Value) {
    let inserted = OffsetDateTime::from_unix_timestamp(inserted.seconds).unwrap()
        + Duration::nanoseconds(inserted.nanos as i64);
    let inserted = inserted.format(&Rfc3339).unwrap();
    println!(
        "{} {} {}",
        idx,
        inserted,
        serde_json::to_string(&payload.into_json()).unwrap(),
    );
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    let mut client = EventsApisClient::connect("http://localhost:40001").await?;
    let x: serde_json::Value = serde_json::from_str("[3.0]").unwrap();
    let s = serde_json::to_string(&x).unwrap();
    let y = x.into_proto();
    let z = y.into_json();
    let s2 = serde_json::to_string(&z).unwrap();
    println!("{} {}", s, s2);
    match args.command {
        Command::Last {} => {
            let GetLastIdxResponse { last_idx } = client
                .get_last_idx(Request::new(GetLastIdxRequest {}))
                .await?
                .into_inner();
            println!("{}", last_idx);
        }
        Command::Get { idx } => {
            let GetEventResponse {
                found,
                inserted,
                payload,
            } = client
                .get_event(Request::new(GetEventRequest { idx }))
                .await?
                .into_inner();
            if !found {
                eprintln!("not found");
                exit(-1);
            } else {
                print_message(idx, inserted.unwrap(), payload.unwrap());
            }
        }
        Command::Put { idx, payload } => {
            let payload: serde_json::Value = serde_json::from_str(&payload)?;
            let InsertEventResponse { success } = client
                .insert_event(Request::new(InsertEventRequest {
                    idx,
                    payload: Some(payload.into_proto()),
                }))
                .await?
                .into_inner();
            if success {
                println!("success");
            } else {
                eprintln!("failed");
                exit(-1);
            }
        }
        Command::Tail { idx } => {
            let mut stream = client
                .poll_events(Request::new(PollEventsRequest { last_idx: idx }))
                .await?
                .into_inner();
            loop {
                match stream.message().await? {
                    Some(message) => {
                        let PollEventsResponse {
                            idx,
                            inserted,
                            payload,
                        } = message;
                        print_message(idx, inserted.unwrap(), payload.unwrap());
                    }
                    None => break,
                }
            }
        }
    }
    Ok(())
}

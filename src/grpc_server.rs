use crate::{message::Message, pgpool::PgPool};
use anystruct::{IntoJSON, IntoProto};
use eventsapis_proto::{
    events_apis_server::{EventsApis, EventsApisServer},
    GetEventRequest, GetEventResponse, GetLastIdxRequest, GetLastIdxResponse, InsertEventRequest,
    InsertEventResponse, PollEventsRequest, PollEventsResponse,
};
use futures::pin_mut;
use log::trace;
use std::{sync::Arc, time::SystemTime};
use tokio::sync::{
    broadcast::{error::RecvError, Receiver},
    mpsc,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

struct GrpcServer {
    pool: Arc<PgPool>,
    msgrx: Receiver<Arc<Message>>,
}

impl GrpcServer {
    pub fn new(pool: Arc<PgPool>, msgrx: Receiver<Arc<Message>>) -> GrpcServer {
        GrpcServer { pool, msgrx }
    }
}

#[tonic::async_trait]
impl EventsApis for GrpcServer {
    type PollEventsStream = ReceiverStream<Result<PollEventsResponse, Status>>;

    async fn get_last_idx(
        &self,
        _request: Request<GetLastIdxRequest>,
    ) -> Result<Response<GetLastIdxResponse>, Status> {
        match self.pool.try_get_last_index().await {
            Ok(idx) => Ok(Response::new(GetLastIdxResponse { last_idx: idx })),
            Err(error) => Err(Status::unavailable(error.to_string())),
        }
    }

    async fn insert_event(
        &self,
        request: Request<InsertEventRequest>,
    ) -> Result<Response<InsertEventResponse>, Status> {
        let InsertEventRequest { idx, payload } = request.into_inner();
        match payload {
            None => Err(Status::invalid_argument("payload is missing")),
            Some(payload) => match self.pool.try_insert(idx, payload.into_json()).await {
                Ok(success) => Ok(Response::new(InsertEventResponse { success })),
                Err(error) => Err(Status::unknown(error.to_string())),
            },
        }
    }

    async fn get_event(
        &self,
        request: Request<GetEventRequest>,
    ) -> Result<Response<GetEventResponse>, Status> {
        let GetEventRequest { idx } = request.into_inner();
        match self.pool.try_fetch(idx).await {
            Ok(Some((inserted, payload))) => Ok(Response::new(GetEventResponse {
                found: true,
                inserted: Some(inserted.into()),
                payload: Some(payload.into_proto()),
            })),
            Ok(None) => Ok(Response::new(GetEventResponse {
                found: false,
                inserted: None,
                payload: None,
            })),
            Err(error) => Err(Status::unknown(error.to_string())),
        }
    }

    async fn poll_events(
        &self,
        request: Request<PollEventsRequest>,
    ) -> Result<Response<Self::PollEventsStream>, Status> {
        let PollEventsRequest { last_idx } = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let msgrx = self.msgrx.resubscribe();
        let pool = self.pool.clone();
        tokio::spawn(async move { handle_poll_events(pool, last_idx, tx, msgrx).await });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn select_phase(
    mut last_idx: i64,
    pool: Arc<PgPool>,
    tx: &mpsc::Sender<Result<PollEventsResponse, Status>>,
) -> Result<Option<i64>, tokio_postgres::Error> {
    let client = pool.get_client().await?;
    let stream = client
        .query_raw(
            "select idx, inserted, payload from events where idx > $1 order by idx",
            &[&last_idx],
        )
        .await?;
    pin_mut!(stream);
    loop {
        match stream.try_next().await? {
            None => break,
            Some(row) => {
                let idx = row.get(0);
                assert!(idx == last_idx + 1);
                let inserted: SystemTime = row.get(1);
                let payload: serde_json::Value = row.get(2);
                let result = tx
                    .send(Ok(PollEventsResponse {
                        idx,
                        inserted: Some(inserted.into()),
                        payload: Some(payload.into_proto()),
                    }))
                    .await;
                match result {
                    Ok(()) => {
                        last_idx = idx;
                    }
                    Err(error) => {
                        trace!("error sending to client: {}", error);
                        return Ok(None);
                    }
                }
            }
        }
    }
    pool.put_client(client).await;
    Ok(Some(last_idx))
}

async fn stream_phase(
    mut last_idx: i64,
    msgrx: &mut Receiver<Arc<Message>>,
    tx: &mpsc::Sender<Result<PollEventsResponse, Status>>,
) -> Result<Option<i64>, tokio_postgres::Error> {
    loop {
        match msgrx.recv().await {
            Ok(message) => {
                let idx = message.idx;
                if idx == last_idx + 1 {
                    let result = tx
                        .send(Ok(PollEventsResponse {
                            idx,
                            inserted: Some(message.inserted.into()),
                            payload: Some(message.payload.clone().into_proto()),
                        }))
                        .await;
                    match result {
                        Ok(()) => last_idx = idx,
                        Err(_) => return Ok(None),
                    }
                } else {
                    assert!(idx <= last_idx); // i.e., we didn't miss any
                    continue;
                }
            }
            Err(RecvError::Lagged(_)) => return Ok(Some(last_idx)),
            Err(RecvError::Closed) => unreachable!("sender end should not close"),
        }
    }
}

async fn handle_poll_events(
    pool: Arc<PgPool>,
    mut last_idx: i64,
    tx: mpsc::Sender<Result<PollEventsResponse, Status>>,
    mut msgrx: Receiver<Arc<Message>>,
) -> Result<(), tokio_postgres::Error> {
    loop {
        match select_phase(last_idx, pool.clone(), &tx).await? {
            Some(idx) => match stream_phase(idx, &mut msgrx, &tx).await? {
                Some(idx) => {
                    last_idx = idx;
                    msgrx = msgrx.resubscribe();
                }
                None => break,
            },
            None => break,
        }
    }
    Ok(())
}

pub async fn start(
    pool: Arc<PgPool>,
    msgrx: Receiver<Arc<Message>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:40001".parse()?;
    let server = GrpcServer::new(pool, msgrx);
    Server::builder()
        .add_service(EventsApisServer::new(server))
        .serve(addr)
        .await?;
    Ok(())
}

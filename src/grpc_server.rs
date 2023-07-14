use crate::{
    message::{CommandFinalStatus, EventMessage, QueueMessage},
    pgpool::PgPool,
};
use anystruct::{IntoJSON, IntoProto};
use eventsapis_proto::{
    events_apis_server::{EventsApis, EventsApisServer},
    FinalStatus, GetCommandRequest, GetCommandResponse, GetEventLastIdxRequest,
    GetEventLastIdxResponse, GetEventRequest, GetEventResponse, GetQueueLastIdxRequest,
    GetQueueLastIdxResponse, GetQueueRequest, GetQueueResponse, InsertEventRequest,
    InsertEventResponse, IssueCommandRequest, IssueCommandResponse, PollCommandsRequest,
    PollCommandsResponse, PollEventsRequest, PollEventsResponse, QueueEntry,
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
use uuid::Uuid;

struct GrpcServer {
    pool: Arc<PgPool>,
    eventrx: Receiver<Arc<EventMessage>>,
    queuerx: Receiver<Arc<QueueMessage>>,
}

impl GrpcServer {
    pub fn new(
        pool: Arc<PgPool>,
        eventrx: Receiver<Arc<EventMessage>>,
        queuerx: Receiver<Arc<QueueMessage>>,
    ) -> GrpcServer {
        GrpcServer {
            pool,
            eventrx,
            queuerx,
        }
    }
}

#[tonic::async_trait]
impl EventsApis for GrpcServer {
    type PollEventsStream = ReceiverStream<Result<PollEventsResponse, Status>>;
    type PollCommandsStream = ReceiverStream<Result<PollCommandsResponse, Status>>;

    async fn get_event_last_idx(
        &self,
        _request: Request<GetEventLastIdxRequest>,
    ) -> Result<Response<GetEventLastIdxResponse>, Status> {
        match self.pool.try_get_event_last_index().await {
            Ok(idx) => Ok(Response::new(GetEventLastIdxResponse { last_idx: idx })),
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
        match self.pool.try_get_event(idx).await {
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
        let eventrx = self.eventrx.resubscribe();
        let pool = self.pool.clone();
        tokio::spawn(async move { handle_poll_events(pool, last_idx, tx, eventrx).await });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn issue_command(
        &self,
        request: Request<IssueCommandRequest>,
    ) -> Result<Response<IssueCommandResponse>, Status> {
        let IssueCommandRequest {
            command_id,
            command_type,
            command_data,
        } = request.into_inner();
        let command_id = Uuid::parse_str(&command_id)
            .map_err(|_e| Status::new(tonic::Code::InvalidArgument, "invalid uuid format"))?;
        let command_data = command_data
            .ok_or_else(|| Status::new(tonic::Code::InvalidArgument, "missing command data"))?;
        let already_exists = self
            .pool
            .try_issue_command(command_id, command_type, command_data.into_json())
            .await
            .map_err(|_| Status::new(tonic::Code::Internal, "failed to insert"))?;
        Ok(Response::new(IssueCommandResponse { already_exists }))
    }

    async fn get_command(
        &self,
        request: Request<GetCommandRequest>,
    ) -> Result<Response<GetCommandResponse>, Status> {
        let GetCommandRequest { command_id } = request.into_inner();
        let command_id = Uuid::parse_str(&command_id)
            .map_err(|_| Status::invalid_argument("invalid uuid format"))?;
        match self.pool.try_get_command(command_id).await {
            Ok(Some((command_type, command_data, inserted, status))) => {
                Ok(Response::new(GetCommandResponse {
                    command_type: Some(command_type),
                    command_data: Some(command_data.into_proto()),
                    inserted: Some(inserted.into()),
                    status: match status {
                        Some(CommandFinalStatus::Succeeded) => Some(FinalStatus::Succeeded.into()),
                        Some(CommandFinalStatus::Failed) => Some(FinalStatus::Failed.into()),
                        Some(CommandFinalStatus::Aborted) => Some(FinalStatus::Aborted.into()),
                        None => None,
                    },
                }))
            }
            Ok(None) => Ok(Response::new(GetCommandResponse {
                command_type: None,
                command_data: None,
                inserted: None,
                status: None,
            })),
            Err(error) => Err(Status::unknown(error.to_string())),
        }
    }

    async fn get_queue_last_idx(
        &self,
        _request: Request<GetQueueLastIdxRequest>,
    ) -> Result<Response<GetQueueLastIdxResponse>, Status> {
        match self.pool.try_get_queue_last_index().await {
            Ok(idx) => Ok(Response::new(GetQueueLastIdxResponse { last_idx: idx })),
            Err(error) => Err(Status::unavailable(error.to_string())),
        }
    }

    async fn poll_commands(
        &self,
        _request: Request<PollCommandsRequest>,
    ) -> Result<Response<Self::PollCommandsStream>, Status> {
        todo!();
    }

    async fn get_queue(
        &self,
        _: Request<GetQueueRequest>,
    ) -> Result<Response<GetQueueResponse>, Status> {
        let result = self
            .pool
            .get_queue()
            .await
            .map_err(|_| Status::new(tonic::Code::Internal, "failed to query"))?;
        let entries = result
            .into_iter()
            .map(
                |(command_id, command_type, command_data, inserted)| QueueEntry {
                    command_id: command_id.to_string(),
                    command_type,
                    command_data: Some(command_data.into_proto()),
                    inserted: Some(inserted.into()),
                },
            )
            .collect();
        Ok(Response::new(GetQueueResponse { entries }))
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
    eventrx: &mut Receiver<Arc<EventMessage>>,
    tx: &mpsc::Sender<Result<PollEventsResponse, Status>>,
) -> Result<Option<i64>, tokio_postgres::Error> {
    loop {
        match eventrx.recv().await {
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
    mut eventrx: Receiver<Arc<EventMessage>>,
) -> Result<(), tokio_postgres::Error> {
    loop {
        match select_phase(last_idx, pool.clone(), &tx).await? {
            Some(idx) => match stream_phase(idx, &mut eventrx, &tx).await? {
                Some(idx) => {
                    last_idx = idx;
                    eventrx = eventrx.resubscribe();
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
    eventrx: Receiver<Arc<EventMessage>>,
    queuerx: Receiver<Arc<QueueMessage>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let jh1 = tokio::spawn({
        let eventrx = eventrx.resubscribe();
        let queuerx = queuerx.resubscribe();
        let pool = pool.clone();
        async {
            let addr = "0.0.0.0:40001".parse().unwrap();
            let server = GrpcServer::new(pool, eventrx, queuerx);
            Server::builder()
                .add_service(EventsApisServer::new(server))
                .serve(addr)
                .await
                .unwrap();
        }
    });
    let jh2 = tokio::spawn(async {
        let addr = "0.0.0.0:40002".parse().unwrap();
        let server = GrpcServer::new(pool, eventrx, queuerx);
        Server::builder()
            .accept_http1(true)
            .add_service(tonic_web::enable(EventsApisServer::new(server)))
            .serve(addr)
            .await
            .unwrap();
    });
    jh1.await.unwrap();
    jh2.await.unwrap();
    Ok(())
}

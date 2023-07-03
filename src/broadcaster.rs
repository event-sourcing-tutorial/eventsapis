use crate::{message::Message, pgpool::PgPool};
use futures::StreamExt;
use futures_util::{pin_mut, TryStreamExt};
use log::{error, info, trace};
use std::{collections::VecDeque, fmt::Debug, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    select,
    sync::{
        broadcast,
        mpsc::{unbounded_channel, UnboundedReceiver},
    },
    time::sleep,
};
use tokio_postgres::{AsyncMessage, Client, Config, Connection, NoTls};

type Chan<M> = broadcast::Sender<Arc<M>>;

enum ConnState<M> {
    Buffering { buffer: VecDeque<M> },
    Streaming { idx: Option<i64>, chan: Chan<M> },
}

async fn poll_and_broadcast<
    M: Message + Send + Sync + Debug,
    S: Unpin + AsyncRead + AsyncWrite,
    T: Unpin + AsyncRead + AsyncWrite,
>(
    mut connection: Connection<S, T>,
    mut connrx: UnboundedReceiver<(Option<i64>, Chan<M>, bool)>,
    pool: &PgPool,
) -> (Option<i64>, Chan<M>) {
    let mut stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
    let mut state = ConnState::Buffering {
        buffer: VecDeque::new(),
    };
    loop {
        select! {
            message = connrx.recv() => {
                let (idx, chan, error) = message.unwrap();
                let mut buffer = match state {
                    ConnState::Buffering { buffer } => buffer,
                    ConnState::Streaming { .. } => unreachable!(),
                };
                if error {
                    state = ConnState::Streaming { idx, chan };
                    break;
                } else {
                    let mut idx = idx;
                    while let Some(message) = buffer.pop_front() {
                        if idx.is_none() || idx.is_some_and(|idx| message.get_idx() > idx) {
                            idx = Some(message.get_idx());
                            trace!("broadcasting {:?}", message);
                            chan.send(Arc::new(message)).unwrap();
                        }
                    }
                    state = ConnState::Streaming { idx, chan };
                }
            }
            result = stream.next() => {
                match result {
                    None => {
                        trace!("loop exiting");
                        break;
                    }
                    Some(Err(error)) => {
                        error!("Stream Error: {}", error);
                        break;
                    }
                    Some(Ok(AsyncMessage::Notification(notification))) => {
                        let message = M::from_str(notification.payload(), pool).await;
                        match state {
                            ConnState::Buffering { ref mut buffer } => {
                                trace!("buffering {:?}", message);
                                buffer.push_back(message);
                            }
                            ConnState::Streaming {ref mut idx, ref chan} => {
                                *idx = Some(message.get_idx());
                                trace!("broadcasting {:?}", message);
                                chan.send(Arc::new(message)).unwrap();
                            }
                        }
                    }
                    Some(Ok(message)) => {
                        info!("notice: {:?}", message);
                    }
                }
            }
        }
    }
    match state {
        ConnState::Streaming { idx, chan } => (idx, chan),
        ConnState::Buffering { .. } => {
            let (idx, chan, _error) = connrx.recv().await.unwrap();
            (idx, chan)
        }
    }
}

async fn select_and_broadcast<M: Message + Send + Sync + Debug>(
    idx: Option<i64>,
    chan: Chan<M>,
    client: &Client,
) -> (Option<i64>, Chan<M>, bool) {
    let mut idx = idx;
    let error = match client.execute(M::listen_query(), &[]).await {
        Ok(_) => match client.query_raw(M::select_query(), &[&idx]).await {
            Ok(stream) => {
                pin_mut!(stream);
                loop {
                    match stream.try_next().await {
                        Ok(Some(row)) => {
                            let message = M::from_row(row);
                            trace!("selected message {:?}", message);
                            idx = Some(message.get_idx());
                            chan.send(Arc::new(message)).unwrap();
                        }
                        Ok(None) => break None,
                        Err(error) => break Some(error),
                    }
                }
            }
            Err(error) => Some(error),
        },
        Err(error) => Some(error),
    };
    if let Some(ref error) = error {
        error!("Error: {:?}", error);
    }
    (idx, chan, error.is_some())
}

async fn tail_once<M: Message + Send + Sync + Debug + 'static>(
    idx: Option<i64>,
    chan: Chan<M>,
    config: &Config,
    pool: Arc<PgPool>,
) -> (Option<i64>, Chan<M>) {
    match config.connect(NoTls).await {
        Ok((client, connection)) => {
            trace!("connected");
            let (conntx, connrx) = unbounded_channel();
            let jh =
                tokio::spawn(async move { poll_and_broadcast(connection, connrx, &pool).await });
            let (idx, chan, error) = select_and_broadcast(idx, chan, &client).await;
            conntx.send((idx, chan, error)).unwrap();
            jh.await.unwrap()
        }
        Err(error) => {
            error!("Connect Error: {}", error);
            (idx, chan)
        }
    }
}

pub fn start_broadcaster<M: Message + Send + Sync + Debug + 'static>(
    config: Config,
    pool: Arc<PgPool>,
) -> broadcast::Receiver<Arc<M>> {
    let (msgtx, msgrx) = broadcast::channel(16);
    tokio::spawn(async move {
        let mut idx = Some(32);
        let mut msgtx = msgtx;
        loop {
            (idx, msgtx) = tail_once(idx, msgtx, &config, pool.clone()).await;
            trace!("idx advanced to {:?}", idx);
            sleep(Duration::from_millis(200)).await;
        }
    });
    msgrx
}

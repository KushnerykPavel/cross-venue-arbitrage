use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

pub(crate) enum IncomingFrame {
    Text(String),
    Ping(Vec<u8>),
    Closed(String),
    Other,
}

pub(crate) enum OutgoingFrame {
    Text(String),
    Pong(Vec<u8>),
    Ping(Vec<u8>),
}

#[allow(async_fn_in_trait)]
pub(crate) trait Transport {
    type Connection: TransportConnection;

    async fn connect(&mut self, endpoint: &str) -> Result<Self::Connection, String>;
}

#[allow(async_fn_in_trait)]
pub(crate) trait TransportConnection {
    async fn receive(&mut self) -> Result<Option<IncomingFrame>, String>;
    async fn send(&mut self, frame: OutgoingFrame) -> Result<(), String>;
    async fn close(&mut self);
}

#[derive(Default)]
pub(crate) struct TungsteniteTransport;

impl Transport for TungsteniteTransport {
    type Connection = TungsteniteConnection;

    async fn connect(&mut self, endpoint: &str) -> Result<Self::Connection, String> {
        let (socket, _) = connect_async(endpoint)
            .await
            .map_err(|error| error.to_string())?;
        Ok(TungsteniteConnection(socket))
    }
}

pub(crate) struct TungsteniteConnection(WebSocketStream<MaybeTlsStream<TcpStream>>);

impl TransportConnection for TungsteniteConnection {
    async fn receive(&mut self) -> Result<Option<IncomingFrame>, String> {
        let Some(message) = self.0.next().await else {
            return Ok(None);
        };
        message
            .map(|message| match message {
                Message::Text(text) => IncomingFrame::Text(text.to_string()),
                Message::Ping(payload) => IncomingFrame::Ping(payload.to_vec()),
                Message::Close(frame) => IncomingFrame::Closed(format!("{frame:?}")),
                _ => IncomingFrame::Other,
            })
            .map(Some)
            .map_err(|error| error.to_string())
    }

    async fn send(&mut self, frame: OutgoingFrame) -> Result<(), String> {
        let message = match frame {
            OutgoingFrame::Text(text) => Message::Text(text.into()),
            OutgoingFrame::Pong(payload) => Message::Pong(payload.into()),
            OutgoingFrame::Ping(payload) => Message::Ping(payload.into()),
        };
        self.0
            .send(message)
            .await
            .map_err(|error| error.to_string())
    }

    async fn close(&mut self) {
        let _ = self.0.close(None).await;
    }
}

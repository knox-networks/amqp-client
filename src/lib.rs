use std::{env, future::Future, io, time::Duration};

use bigerror::{LogError, Report, ReportAs};
use futures_util::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    tcp::{AMQPUriTcpExt, HandshakeError, NativeTlsConnector},
    types::FieldTable,
    uri::{AMQPScheme, AMQPUri},
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use tokio::sync::{mpsc, mpsc::UnboundedSender};
use tracing::*;

#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;

#[derive(Debug, thiserror::Error)]
#[error("AmqpError")]
pub struct AmqpError;
bigerror::reportable!(AmqpError);

#[derive(Debug, Clone)]
pub struct AmqpConnectionConfig {
    pub uri: AMQPUri,
    pub connection: String,
    pub exchange: String,
    pub inbound_queue: String,
    pub outbound_queue: String,
    pub consumer_name: String,
}

pub trait AmqpMessageHandler: Clone + Send + Sync + 'static {
    type Input;

    fn prepare_payload(&self, input: Self::Input) -> Option<Vec<u8>>;
    fn receive_message(
        &self,
        message: &[u8],
    ) -> impl Future<Output = Result<(), Report<AmqpError>>> + Send;
}

pub struct AmqpClient<H>
where
    H: AmqpMessageHandler,
{
    conf: AmqpConnectionConfig,
    message_handler: H,
}

impl<H> AmqpClient<H>
where
    H: AmqpMessageHandler,
{
    pub fn new(conf: AmqpConnectionConfig, message_handler: H) -> Self {
        Self {
            conf,
            message_handler,
        }
    }

    pub fn init(&self) -> UnboundedSender<H::Input>
    where
        <H as AmqpMessageHandler>::Input: Send + Sync + 'static,
    {
        let (input_tx, mut input_rx) = mpsc::unbounded_channel::<H::Input>();

        let uri = self.conf.uri.clone();
        let connection = self.conf.connection.clone();
        let exchange = self.conf.exchange.clone();
        let outbound_queue = self.conf.outbound_queue.clone();
        let inbound_queue = self.conf.inbound_queue.clone();
        let consumer_name = self.conf.consumer_name.clone();
        let message_handler = self.message_handler.clone();

        tokio::spawn(async move {
            loop {
                debug!(spawning = "AmqpClient.init_task");

                let chan = amqp_connect(&uri, &connection).await;

                chan.queue_declare(
                    &outbound_queue,
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await
                .report_as()?;

                chan.queue_declare(
                    &inbound_queue,
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await
                .report_as()?;

                let mut consumer = chan
                    .basic_consume(
                        &inbound_queue,
                        &consumer_name,
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                    .report_as()?;

                loop {
                    tokio::select! {
                        Some(delivery_res) = consumer.next() => {
                            let delivery = match delivery_res
                                .and_attached_err("failed to consume next ampq message")
                            {
                                Ok(delivery) => delivery,
                                Err(lapin::Error::ProtocolError(_) | lapin::Error::IOError(_)) => {
                                    break;
                                }
                                // TODO Determine how to handle other error types
                                _ => {
                                    continue;
                                }
                            };

                            if delivery
                                .ack(BasicAckOptions::default())
                                .await
                                .and_log_err()
                                .is_err()
                            {
                                continue;
                            }

                            // call handler method
                            let _ = message_handler
                                .receive_message(&delivery.data)
                                .await
                                .and_log_err();
                        },
                        Some(notification) = input_rx.recv() => {
                            let Some(payload) = message_handler.prepare_payload(notification) else {
                                continue;
                            };

                            // TODO Determine the correct action to perform if this errors
                            let Ok(basic_pub) = chan
                                .basic_publish(
                                    &exchange,
                                    &outbound_queue,
                                    BasicPublishOptions::default(),
                                    &payload,
                                    BasicProperties::default(),
                                )
                                .await
                                .and_log_err()
                            else {
                                continue;
                            };

                            // TODO Determine the correct action to perform if this errors
                            let Ok(_promise) = basic_pub.await.and_log_err() else {
                                continue;
                            };
                        },
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                    }
                }
            }

            #[allow(unreachable_code)]
            Ok::<(), Report<AmqpError>>(())
        });

        input_tx
    }
}

pub async fn amqp_connect(uri: &AMQPUri, conn_name: &str) -> Channel {
    let mut attempt: u64 = 1;
    // Cap exponentiation attempts to 11 or 121 seconds delay max interval (~2 minutes re-attempt)
    let attempts_ceiling = 11;

    loop {
        info!(?attempt);

        match connect(uri.clone(), conn_name).await {
            Err(e) => {
                info!(err = %e, "Failed to establish connection");

                let delay = attempt.pow(2);
                info!("Retrying in {delay} seconds...");
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                // set increment back attempt back to 1 once ceiling is hit
                attempt = (attempt % attempts_ceiling) + 1;
            }
            Ok(channel) => return channel,
        }
    }
}

#[instrument(skip(uri))]
pub async fn connect(mut uri: AMQPUri, conn_name: &str) -> Result<Channel, lapin::Error> {
    let amqp_force_insecure = env::var("AMQP_FORCE_INSECURE")
        .map(|v| v.parse::<bool>().unwrap_or(false))
        .unwrap_or(false);
    let connection_properties =
        ConnectionProperties::default().with_connection_name(conn_name.into());

    let conn = if amqp_force_insecure {
        uri.scheme = AMQPScheme::AMQP;

        let connect = move |uri: &AMQPUri| {
            uri.connect().and_then(|stream| {
                let tls_connector = NativeTlsConnector::builder()
                    .danger_accept_invalid_hostnames(true)
                    .build()
                    .map_err(|e| {
                        HandshakeError::Failure(io::Error::new(io::ErrorKind::Other, e))
                    })?;
                stream.into_native_tls(&tls_connector, &uri.authority.host)
            })
        };

        Connection::connector(uri, Box::new(connect), connection_properties).await?
    } else {
        Connection::connect_uri(uri, connection_properties).await?
    };

    let chan = conn.create_channel().await?;

    Ok(chan)
}

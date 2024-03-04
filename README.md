# AMQP client

This crate provides a simple AMQP client capable of processing messages of a
defined type. The client provides an unbounded channel to interact with the
client, either to submit new messages to the queue or to receive messages from
the queue.

### Usage

```rust
const CONNECTION: &str = "my_service";
const EXCHANGE: &str = "my_exchange";
const REPLY_QUEUE: &str = "queue_reply";
const REQUEST_QUEUE: &str = "queue_request";
const CONSUMER_NAME: &str = "my_consumer_name";

// define a handler for incoming and outgoing messages
struct MyMessageHandler {}

impl AmqpMessageHandler for MyMessageHandler {
    // the type of the message
    type Input = Payload;

    async fn receive_message(&self, message: &[u8]) -> Result<(), Report<AmqpError>> {
        // parse the message bytes and do something
        let Ok(payload) = Payload::from_amqp_payload(message).and_log_err() else {
            return Ok(());
        };

        // do something with payload

        Ok(())
    }

    fn prepare_payload(&self, input: Self::Input) -> Option<Vec<u8>> {
        // convert payload back to bytes
        Some(input.encode_to_vec())
    }
}

async fn init(uri: AMQPUri) {
    let message_handler = MyMessageHandler {};

    let amqp_client = AmqpClient::new(
        AmqpConnectionConfig {
            uri,
            connection: CONNECTION.to_string(),
            exchange: EXCHANGE.to_string(),
            outbound_queue: REPLY_QUEUE.to_string(),
            inbound_queue: REQUEST_QUEUE.to_string(),
            consumer_name: CONSUMER_NAME.to_string(),
        },
        message_handler,
    );

    let input_tx = amqp_client.init();

    // do something with input_tx, or just hang on to it
}

```

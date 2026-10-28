# datastream-kafka

Kafka producer and consumer streams.

- `kafka_connect`
- `kafka_produce_stream`
- `kafka_consume_stream`: returns the stream and a `KafkaConsumeStop`. `stop()`
  ends the stream, waits for every open `kafka_commit_stream` on the same
  consumer to finish its final commit, then stops the consumer. Cancelling the
  `signal` aborts instead: the consumer stops first, so nothing pending is
  committed.
- `kafka_commit_stream`: commits `offset + 1` per topic-partition every
  `batch_size` messages (default 100) and once more at the end.

Enable the `rdkafka` feature for `RdKafka`, a client backed by `rdkafka`.

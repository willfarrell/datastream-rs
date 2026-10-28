# datastream-schema-registry

Confluent and AWS Glue Schema Registry wire-format framing streams, and a
Confluent schema resolver.

- `confluent_frame_stream`
- `confluent_unframe_stream`
- `glue_frame_stream`
- `glue_unframe_stream`
- `confluent_schema_registry_resolver` (`resolver` feature)

Each chunk is one whole frame. Unframe streams also accept a message carrying
the frame in its value (`FramedChunk`); the message rides along on the
envelope. Enable the `kafka` feature to unframe `datastream-kafka` consumer
messages.

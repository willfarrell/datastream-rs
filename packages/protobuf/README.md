# datastream-protobuf

Protocol Buffers encode/decode and length-prefix framing streams.

- `protobuf_encode_stream`
- `protobuf_decode_stream`: each message is capped by `max_message_size`
  (default 64MiB; `usize::MAX` for none). The cap is per message, not a
  running total.
- `protobuf_length_prefix_frame_stream`
- `protobuf_length_prefix_unframe_stream`: rejects a declared length over
  `max_message_size` (default 64MiB) before buffering the body, and a varint
  length prefix longer than 10 bytes.

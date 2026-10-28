# datastream-charset

Character encoding detection, decoding, and conversion streams.

- `charset_detect_stream`
- `charset_decode_stream` (errors on an unknown charset; none means UTF-8)
- `charset_encode_stream` (errors on an unknown charset; none means UTF-8;
  unmappable characters become `?`)

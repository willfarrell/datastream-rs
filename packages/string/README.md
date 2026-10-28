# datastream-string

String manipulation streams, ported from `@datastream/string`.

- `string_length_stream`
- `string_count_stream` (result key `stringCount`)
- `string_minimum_first_chunk_size_stream`
- `string_minimum_chunk_size_stream`
- `string_skip_consecutive_duplicates_stream`
- `string_replace_stream`: string or regex `pattern`, replaced as if the
  stream were one string. `replacement` is a JS `String.prototype.replace`
  template (`$$`, `$&`, `` $` ``, `$'`, `$n`, `$<name>`) or a function of the
  match. Options: `max_match_length`, `lookbehind`, `max_buffer_size`.
- `string_split_stream`

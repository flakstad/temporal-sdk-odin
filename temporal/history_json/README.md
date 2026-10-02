# Temporal History JSON conversion

This optional package converts the canonical protobuf JSON emitted by
`temporal workflow show --output json` into the binary
`temporal.api.history.v1.History` message accepted by SDK Core's offline
replayer.

```odin
protobuf_bytes, err := history_json.to_protobuf(json_bytes)
defer delete(protobuf_bytes)
defer history_json.error_free(&err)
```

The caller owns both returned slices. Input is borrowed only for the call. The
implementation uses a small native sidecar generated from the repository's
pinned Temporal API descriptors; it is unrelated to application Payload
codecs. Unknown fields fail conversion rather than being discarded.

Only programs importing this package link
`libtemporal_odin_history_json.{dylib,so}`. The library and matching C header
are included in the native package beside SDK Core.

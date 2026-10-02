use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use std::{panic, ptr, slice, sync::OnceLock};

const HISTORY_MESSAGE_NAME: &str = "temporal.api.history.v1.History";
const DESCRIPTORS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/history-descriptors.bin"));

static DESCRIPTOR_POOL: OnceLock<Result<DescriptorPool, String>> = OnceLock::new();

#[repr(C)]
pub struct OwnedBytes {
    data: *mut u8,
    size: usize,
    capacity: usize,
}

#[repr(C)]
pub struct ConversionResult {
    protobuf: *mut OwnedBytes,
    fail: *mut OwnedBytes,
}

impl OwnedBytes {
    fn into_raw(bytes: Vec<u8>) -> *mut Self {
        let mut bytes = std::mem::ManuallyDrop::new(bytes);
        Box::into_raw(Box::new(Self {
            data: bytes.as_mut_ptr(),
            size: bytes.len(),
            capacity: bytes.capacity(),
        }))
    }
}

fn history_descriptor() -> Result<MessageDescriptor, String> {
    let pool = DESCRIPTOR_POOL.get_or_init(|| {
        DescriptorPool::decode(DESCRIPTORS)
            .map_err(|error| format!("could not load pinned Temporal descriptors: {error}"))
    });
    match pool {
        Ok(pool) => pool
            .get_message_by_name(HISTORY_MESSAGE_NAME)
            .ok_or_else(|| format!("pinned descriptors do not contain {HISTORY_MESSAGE_NAME}")),
        Err(message) => Err(message.clone()),
    }
}

fn convert(json: &[u8]) -> Result<Vec<u8>, String> {
    if json.is_empty() {
        return Err("history JSON must not be empty".to_owned());
    }
    let descriptor = history_descriptor()?;
    let mut deserializer = serde_json::Deserializer::from_slice(json);
    let history = DynamicMessage::deserialize(descriptor, &mut deserializer)
        .map_err(|error| format!("history JSON decode failed: {error}"))?;
    deserializer
        .end()
        .map_err(|error| format!("history JSON has trailing data: {error}"))?;
    Ok(history.encode_to_vec())
}

fn failure(message: String) -> ConversionResult {
    ConversionResult {
        protobuf: ptr::null_mut(),
        fail: OwnedBytes::into_raw(message.into_bytes()),
    }
}

/// Convert canonical protobuf JSON for temporal.api.history.v1.History to its
/// binary protobuf representation. The result owns exactly one non-null buffer.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_odin_history_json_to_protobuf(
    data: *const u8,
    size: usize,
) -> ConversionResult {
    if data.is_null() && size != 0 {
        return failure("history JSON pointer is null while size is non-zero".to_owned());
    }
    let converted = panic::catch_unwind(|| {
        let input = if size == 0 {
            &[]
        } else {
            // SAFETY: The caller lends exactly size readable bytes for this synchronous call.
            unsafe { slice::from_raw_parts(data, size) }
        };
        convert(input)
    });
    match converted {
        Ok(Ok(protobuf)) => ConversionResult {
            protobuf: OwnedBytes::into_raw(protobuf),
            fail: ptr::null_mut(),
        },
        Ok(Err(message)) => failure(message),
        Err(_) => failure("history JSON conversion panicked".to_owned()),
    }
}

/// Free a buffer returned by temporal_odin_history_json_to_protobuf.
#[unsafe(no_mangle)]
pub extern "C" fn temporal_odin_history_json_bytes_free(bytes: *mut OwnedBytes) {
    if bytes.is_null() {
        return;
    }
    // SAFETY: This function accepts each bridge-owned pointer at most once.
    let bytes = unsafe { Box::from_raw(bytes) };
    if bytes.capacity != 0 {
        // SAFETY: data, size, and capacity came from the same Vec in OwnedBytes::into_raw.
        unsafe { Vec::from_raw_parts(bytes.data, bytes.size, bytes.capacity) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HISTORY_JSON: &[u8] = br#"{
      "events": [{
        "eventId": "1",
        "eventTime": "2026-01-02T03:04:05.000000006Z",
        "eventType": "EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
        "workflowExecutionStartedEventAttributes": {
          "workflowType": {"name": "odin_json_history_workflow"},
          "taskQueue": {"name": "odin-json-history"},
          "input": {"payloads": [{
            "metadata": {"encoding": "anNvbi9wbGFpbg=="},
            "data": "Ik9kaW4i"
          }]}
        }
      }]
    }"#;

    #[test]
    fn canonical_protobuf_json_converts_to_binary_history() {
        let protobuf = convert(HISTORY_JSON).expect("history should convert");
        let descriptor = history_descriptor().expect("history descriptor should exist");
        let decoded = DynamicMessage::decode(descriptor, protobuf.as_slice())
            .expect("converted history should decode");
        let events = decoded
            .get_field_by_name("events")
            .expect("history should have events");
        assert_eq!(
            events.as_list().expect("events should be repeated").len(),
            1
        );
    }

    #[test]
    fn malformed_or_unknown_json_is_rejected() {
        assert!(convert(br#"{"events": [}"#).is_err());
        assert!(convert(br#"{"unknownPinnedField": true}"#).is_err());
    }

    #[test]
    fn ffi_result_has_single_owner_and_can_be_freed() {
        let result =
            temporal_odin_history_json_to_protobuf(HISTORY_JSON.as_ptr(), HISTORY_JSON.len());
        assert!(!result.protobuf.is_null());
        assert!(result.fail.is_null());
        temporal_odin_history_json_bytes_free(result.protobuf);

        let failed = temporal_odin_history_json_to_protobuf(ptr::null(), 1);
        assert!(failed.protobuf.is_null());
        assert!(!failed.fail.is_null());
        temporal_odin_history_json_bytes_free(failed.fail);
    }
}

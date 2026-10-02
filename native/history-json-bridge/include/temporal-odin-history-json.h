#ifndef TEMPORAL_ODIN_HISTORY_JSON_H
#define TEMPORAL_ODIN_HISTORY_JSON_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TemporalOdinHistoryJsonOwnedBytes {
  uint8_t *data;
  size_t size;
  size_t capacity;
} TemporalOdinHistoryJsonOwnedBytes;

typedef struct TemporalOdinHistoryJsonConversionResult {
  TemporalOdinHistoryJsonOwnedBytes *protobuf;
  TemporalOdinHistoryJsonOwnedBytes *fail;
} TemporalOdinHistoryJsonConversionResult;

TemporalOdinHistoryJsonConversionResult
temporal_odin_history_json_to_protobuf(const uint8_t *data, size_t size);

void temporal_odin_history_json_bytes_free(
    TemporalOdinHistoryJsonOwnedBytes *bytes);

#ifdef __cplusplus
}
#endif

#endif

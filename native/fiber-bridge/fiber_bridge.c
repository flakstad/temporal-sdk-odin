#include <stddef.h>
#include <stdlib.h>

#define MINICORO_IMPL
#include "../minicoro/minicoro.h"

typedef void (*temporal_odin_fiber_entry)(void *user_data);

typedef struct temporal_odin_fiber {
  mco_coro *coroutine;
  temporal_odin_fiber_entry entry;
  void *user_data;
} temporal_odin_fiber;

static void temporal_odin_fiber_trampoline(mco_coro *coroutine) {
  temporal_odin_fiber *fiber =
      (temporal_odin_fiber *)mco_get_user_data(coroutine);
  fiber->entry(fiber->user_data);
}

int temporal_odin_fiber_create(temporal_odin_fiber **out_fiber,
                               temporal_odin_fiber_entry entry,
                               void *user_data, size_t stack_size) {
  temporal_odin_fiber *fiber;
  mco_desc description;
  mco_result result;

  if (out_fiber == NULL || entry == NULL) {
    return MCO_INVALID_ARGUMENTS;
  }
  *out_fiber = NULL;
  fiber = (temporal_odin_fiber *)calloc(1, sizeof(*fiber));
  if (fiber == NULL) {
    return MCO_OUT_OF_MEMORY;
  }
  fiber->entry = entry;
  fiber->user_data = user_data;
  description = mco_desc_init(temporal_odin_fiber_trampoline, stack_size);
  description.user_data = fiber;
  result = mco_create(&fiber->coroutine, &description);
  if (result != MCO_SUCCESS) {
    free(fiber);
    return result;
  }
  *out_fiber = fiber;
  return MCO_SUCCESS;
}

int temporal_odin_fiber_resume(temporal_odin_fiber *fiber) {
  if (fiber == NULL) {
    return MCO_INVALID_COROUTINE;
  }
  return mco_resume(fiber->coroutine);
}

int temporal_odin_fiber_yield(temporal_odin_fiber *fiber) {
  if (fiber == NULL) {
    return MCO_INVALID_COROUTINE;
  }
  return mco_yield(fiber->coroutine);
}

int temporal_odin_fiber_status(temporal_odin_fiber *fiber) {
  if (fiber == NULL) {
    return MCO_DEAD;
  }
  return mco_status(fiber->coroutine);
}

int temporal_odin_fiber_destroy(temporal_odin_fiber *fiber) {
  mco_result result;
  if (fiber == NULL) {
    return MCO_SUCCESS;
  }
  result = mco_destroy(fiber->coroutine);
  if (result == MCO_SUCCESS) {
    free(fiber);
  }
  return result;
}

const char *temporal_odin_fiber_result_description(int result) {
  return mco_result_description((mco_result)result);
}

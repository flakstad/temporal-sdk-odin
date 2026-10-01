#if !defined(_WIN32)
#define _DEFAULT_SOURCE
#endif

#include <stddef.h>
#include <stdlib.h>

#if defined(__APPLE__) || defined(__linux__)
#include <sys/mman.h>
#include <unistd.h>
#define TEMPORAL_ODIN_POSIX_GUARD_PAGES 1
#else
#define TEMPORAL_ODIN_POSIX_GUARD_PAGES 0
#endif

#define MINICORO_IMPL
#include "../minicoro/minicoro.h"

typedef void (*temporal_odin_fiber_entry)(void *user_data);

typedef struct temporal_odin_fiber {
  mco_coro *coroutine;
  temporal_odin_fiber_entry entry;
  void *user_data;
  void *mapping;
  size_t mapping_size;
  size_t guard_offset;
  size_t guard_page_size;
} temporal_odin_fiber;

static size_t temporal_odin_align_forward(size_t value, size_t alignment) {
  return (value + alignment - 1) / alignment * alignment;
}

#if TEMPORAL_ODIN_POSIX_GUARD_PAGES
static void *temporal_odin_guarded_alloc(size_t size, void *allocator_data) {
  temporal_odin_fiber *fiber = (temporal_odin_fiber *)allocator_data;
  size_t mapping_size;
  void *mapping;

  if (fiber == NULL || fiber->guard_page_size == 0 ||
      fiber->guard_offset > size ||
      fiber->guard_page_size > size - fiber->guard_offset) {
    return NULL;
  }
  mapping_size = temporal_odin_align_forward(size, fiber->guard_page_size);
  mapping = mmap(NULL, mapping_size, PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (mapping == MAP_FAILED) {
    return NULL;
  }
  if (mprotect((unsigned char *)mapping + fiber->guard_offset,
               fiber->guard_page_size, PROT_NONE) != 0) {
    (void)munmap(mapping, mapping_size);
    return NULL;
  }
  fiber->mapping = mapping;
  fiber->mapping_size = mapping_size;
  return mapping;
}

static void temporal_odin_guarded_dealloc(void *ptr, size_t size,
                                          void *allocator_data) {
  temporal_odin_fiber *fiber = (temporal_odin_fiber *)allocator_data;
  (void)size;
  if (fiber != NULL && ptr == fiber->mapping && fiber->mapping_size > 0) {
    (void)munmap(fiber->mapping, fiber->mapping_size);
    fiber->mapping = NULL;
    fiber->mapping_size = 0;
  }
}

static int temporal_odin_configure_guarded_description(
    temporal_odin_fiber *fiber, mco_desc *description) {
  long page_size_value;
  size_t page_size;
  size_t aligned_storage_size;
  size_t prefix_size;
  size_t guard_offset;
  size_t guarded_storage_size;

  page_size_value = sysconf(_SC_PAGESIZE);
  if (page_size_value <= 0) {
    return 0;
  }
  page_size = (size_t)page_size_value;
  aligned_storage_size = temporal_odin_align_forward(description->storage_size, 16);
  if (description->coro_size < aligned_storage_size +
                                   description->stack_size + 16) {
    return 0;
  }
  prefix_size = description->coro_size - aligned_storage_size -
                description->stack_size - 16;
  guard_offset = temporal_odin_align_forward(
      prefix_size + aligned_storage_size, page_size);
  guarded_storage_size = guard_offset + page_size - prefix_size;
  if (guarded_storage_size < description->storage_size) {
    return 0;
  }

  description->storage_size = guarded_storage_size;
  description->coro_size = prefix_size + guarded_storage_size +
                           description->stack_size + 16;
  description->alloc_cb = temporal_odin_guarded_alloc;
  description->dealloc_cb = temporal_odin_guarded_dealloc;
  description->allocator_data = fiber;
  fiber->guard_offset = guard_offset;
  fiber->guard_page_size = page_size;
  return 1;
}
#endif

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
#if TEMPORAL_ODIN_POSIX_GUARD_PAGES
  if (!temporal_odin_configure_guarded_description(fiber, &description)) {
    free(fiber);
    return MCO_OUT_OF_MEMORY;
  }
#endif
  result = mco_create(&fiber->coroutine, &description);
  if (result != MCO_SUCCESS) {
    free(fiber);
    return result;
  }
#if TEMPORAL_ODIN_POSIX_GUARD_PAGES
  if (fiber->coroutine->stack_base !=
      (unsigned char *)fiber->mapping + fiber->guard_offset +
          fiber->guard_page_size) {
    (void)mco_destroy(fiber->coroutine);
    free(fiber);
    return MCO_GENERIC_ERROR;
  }
  /* The description temporarily includes padding and the guard page in the
     storage span so minicoro places the stack after it. Keep its public storage
     API bounded to the original accessible region. */
  fiber->coroutine->storage_size = MCO_DEFAULT_STORAGE_SIZE;
#endif
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

size_t temporal_odin_fiber_stack_size(temporal_odin_fiber *fiber) {
  if (fiber == NULL || fiber->coroutine == NULL) {
    return 0;
  }
  return fiber->coroutine->stack_size;
}

int temporal_odin_fiber_has_guard_page(temporal_odin_fiber *fiber) {
  return fiber != NULL && fiber->mapping != NULL &&
         fiber->guard_page_size > 0;
}

size_t temporal_odin_fiber_guard_page_size(temporal_odin_fiber *fiber) {
  if (fiber == NULL || fiber->mapping == NULL) {
    return 0;
  }
  return fiber->guard_page_size;
}

size_t temporal_odin_fiber_reserved_size(temporal_odin_fiber *fiber) {
  if (fiber == NULL) {
    return 0;
  }
  return fiber->mapping_size;
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

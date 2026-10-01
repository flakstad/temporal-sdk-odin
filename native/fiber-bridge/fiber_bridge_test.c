#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

typedef struct temporal_odin_fiber temporal_odin_fiber;
typedef void (*temporal_odin_fiber_entry)(void *user_data);

int temporal_odin_fiber_create(temporal_odin_fiber **out_fiber,
                               temporal_odin_fiber_entry entry,
                               void *user_data, size_t stack_size);
int temporal_odin_fiber_resume(temporal_odin_fiber *fiber);
int temporal_odin_fiber_destroy(temporal_odin_fiber *fiber);
int temporal_odin_fiber_has_guard_page(temporal_odin_fiber *fiber);
size_t temporal_odin_fiber_guard_page_size(temporal_odin_fiber *fiber);
size_t temporal_odin_fiber_reserved_size(temporal_odin_fiber *fiber);

static void normal_entry(void *user_data) {
  int *called = (int *)user_data;
  *called += 1;
}

static volatile int recurse_until_guard = 1;

#if defined(__clang__) || defined(__GNUC__)
__attribute__((noinline))
#endif
static void consume_stack(size_t depth) {
  volatile unsigned char frame[1024];
  size_t index;
  for (index = 0; index < sizeof(frame); ++index) {
    frame[index] = (unsigned char)(depth + index);
  }
  if (recurse_until_guard) {
    consume_stack(depth + frame[depth % sizeof(frame)] + 1);
  }
}

static void overflow_entry(void *user_data) {
  (void)user_data;
  consume_stack(1);
}

static int normal_lifecycle_uses_guard_page(void) {
  temporal_odin_fiber *fiber = NULL;
  int called = 0;
  if (temporal_odin_fiber_create(&fiber, normal_entry, &called, 32 * 1024) !=
      0) {
    return 0;
  }
  if (!temporal_odin_fiber_has_guard_page(fiber) ||
      temporal_odin_fiber_guard_page_size(fiber) == 0 ||
      temporal_odin_fiber_reserved_size(fiber) <= 32 * 1024) {
    return 0;
  }
  if (temporal_odin_fiber_resume(fiber) != 0 || called != 1) {
    return 0;
  }
  return temporal_odin_fiber_destroy(fiber) == 0;
}

int main(void) {
  pid_t child;
  int status;

  if (!normal_lifecycle_uses_guard_page()) {
    fputs("guarded fiber lifecycle failed\n", stderr);
    return 1;
  }
  child = fork();
  if (child < 0) {
    perror("fork");
    return 1;
  }
  if (child == 0) {
    temporal_odin_fiber *fiber = NULL;
    if (temporal_odin_fiber_create(&fiber, overflow_entry, NULL, 32 * 1024) !=
        0) {
      _exit(2);
    }
    (void)temporal_odin_fiber_resume(fiber);
    _exit(3);
  }
  if (waitpid(child, &status, 0) != child) {
    perror("waitpid");
    return 1;
  }
  if (!WIFSIGNALED(status) ||
      (WTERMSIG(status) != SIGSEGV && WTERMSIG(status) != SIGBUS)) {
    fprintf(stderr, "stack overflow did not hit a guard page (status=%d)\n",
            status);
    return 1;
  }
  puts("fiber guard page trapped an isolated stack overflow");
  return 0;
}

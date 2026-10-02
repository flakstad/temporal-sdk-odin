// Package process_start serializes child creation across every SDK process
// manager. Odin's process_start mutates inherited-handle state and is not
// thread-safe, so package-local locks are insufficient when supervisors and
// Workflow/Activity executors coexist.
package process_start

import "core:os"
import "core:sync"

mutex: sync.Mutex

start :: proc(desc: os.Process_Desc) -> (process: os.Process, error: os.Error) {
	sync.mutex_lock(&mutex)
	defer sync.mutex_unlock(&mutex)
	return os.process_start(desc)
}

package kubernetes

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
)

// localLifecycle is an actual single-client-process gate, not a distributed
// lease or an assertion that Kubernetes supports cross-object atomic CAS.
type localLifecycle struct {
	mu    sync.Mutex
	names map[string]*namespaceState
}
type namespaceState struct {
	uid, attempt      string
	writers           int
	draining, unknown bool
	drained           chan struct{}
	operations        map[string]OperationTombstone
}

type OperationTombstone struct {
	ID                string
	Action            string
	Name              string
	UID               string
	ExpectedVersion   string
	TargetRevision    string
	TargetFingerprint string
	WriterEpoch       uint64
	Unknown           bool
}
type LifecycleSnapshot struct {
	Namespace  string
	UID        string
	Attempt    string
	Draining   bool
	Unresolved []OperationTombstone
}

func (c *Client) Lifecycle(name string) LifecycleSnapshot {
	c.lifecycle.mu.Lock()
	defer c.lifecycle.mu.Unlock()
	result := LifecycleSnapshot{Namespace: name}
	if state := c.lifecycle.names[name]; state != nil {
		result.UID, result.Attempt, result.Draining = state.uid, state.attempt, state.draining
		for _, operation := range state.operations {
			result.Unresolved = append(result.Unresolved, operation)
		}
		sort.Slice(result.Unresolved, func(i, j int) bool { return result.Unresolved[i].ID < result.Unresolved[j].ID })
	}
	return result
}

func (l *localLifecycle) run(name, uid, attempt string, operation OperationTombstone, fn func() error) (err error) {
	return l.runMode(name, uid, attempt, operation, false, fn)
}

func (l *localLifecycle) runCleanup(name, uid, attempt string, operation OperationTombstone, fn func() error) error {
	return l.runMode(name, uid, attempt, operation, true, fn)
}

func (l *localLifecycle) runMode(name, uid, attempt string, operation OperationTombstone, cleanup bool, fn func() error) (err error) {
	if operation.ID == "" || strings.TrimSpace(operation.ID) != operation.ID || len(operation.ID) > 256 {
		return failure(InvalidConfiguration, nil)
	}
	l.mu.Lock()
	state := l.names[name]
	if state == nil || state.uid != uid || state.attempt != attempt {
		l.mu.Unlock()
		return failure(AttemptConsumed, nil)
	}
	if state.unknown {
		l.mu.Unlock()
		return failure(UnknownWriteOutcome, nil)
	}
	if state.draining && !cleanup {
		l.mu.Unlock()
		return failure(LifecycleDraining, nil)
	}
	if cleanup && (!state.draining || state.writers > 0) {
		l.mu.Unlock()
		return failure(ConditionalConflict, nil)
	}
	if state.operations == nil {
		state.operations = map[string]OperationTombstone{}
	}
	if _, exists := state.operations[operation.ID]; exists {
		l.mu.Unlock()
		return failure(ConditionalConflict, nil)
	}
	state.operations[operation.ID] = operation
	state.writers++
	l.mu.Unlock()
	defer func() {
		panicked := recover()
		if panicked != nil {
			err = failure(UnknownWriteOutcome, nil)
		}
		l.mu.Lock()
		var outcome *Error
		if errors.As(err, &outcome) && outcome.Code == UnknownWriteOutcome {
			operation.Unknown = true
			state.operations[operation.ID] = operation
			state.unknown = true
		} else {
			delete(state.operations, operation.ID)
		}
		state.writers--
		if state.draining && state.writers == 0 && state.drained != nil {
			close(state.drained)
			state.drained = nil
		}
		l.mu.Unlock()
		if panicked != nil {
			panic(panicked)
		}
	}()
	return fn()
}

func (l *localLifecycle) observe(name, uid, attempt string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.names == nil {
		l.names = map[string]*namespaceState{}
	}
	if current := l.names[name]; current != nil {
		if current.attempt != attempt || current.uid != "" && current.uid != uid {
			return failure(AttemptConsumed, nil)
		}
		if current.unknown {
			return failure(UnknownWriteOutcome, nil)
		}
		if current.draining {
			return failure(LifecycleDraining, nil)
		}
		if current.uid == "" {
			current.uid = uid
		}
		return nil
	}
	l.names[name] = &namespaceState{uid: uid, attempt: attempt}
	return nil
}

func (l *localLifecycle) reserve(name, attempt string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if name == "" || attempt == "" {
		return failure(InvalidConfiguration, nil)
	}
	if l.names == nil {
		l.names = map[string]*namespaceState{}
	}
	if current := l.names[name]; current != nil {
		if current.attempt != attempt {
			return failure(AttemptConsumed, nil)
		}
		if current.unknown {
			return failure(UnknownWriteOutcome, nil)
		}
		if current.uid != "" || current.draining {
			return failure(AttemptConsumed, nil)
		}
		return nil
	}
	l.names[name] = &namespaceState{attempt: attempt}
	return nil
}

// restoreDraining never resets an existing lifecycle or clears uncertainty.
// Durable admission and old-process drain are the caller's separate contract.
func (l *localLifecycle) restoreDraining(name, uid, attempt string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if name == "" || uid == "" || attempt == "" {
		return failure(InvalidConfiguration, nil)
	}
	if l.names == nil {
		l.names = map[string]*namespaceState{}
	}
	if current := l.names[name]; current != nil {
		if current.uid != uid || current.attempt != attempt {
			return failure(AttemptConsumed, nil)
		}
		current.draining = true
		if current.unknown {
			return failure(UnknownWriteOutcome, nil)
		}
		if current.writers != 0 {
			return failure(LifecycleDraining, nil)
		}
		return nil
	}
	l.names[name] = &namespaceState{uid: uid, attempt: attempt, draining: true}
	return nil
}

// drain is irreversible locally: cancellation does not reopen the namespace.
// It drains callbacks, not an assertion that an ambiguous server request ended.
// Unknown results keep tombstones and prevent teardown/finalizer removal.
func (l *localLifecycle) drain(ctx context.Context, name, uid, attempt string) error {
	l.mu.Lock()
	state := l.names[name]
	if state == nil || state.uid != uid || state.attempt != attempt {
		l.mu.Unlock()
		return failure(AttemptConsumed, nil)
	}
	state.draining = true
	if state.writers == 0 {
		unknown := state.unknown
		l.mu.Unlock()
		if unknown {
			return failure(UnknownWriteOutcome, nil)
		}
		return nil
	}
	if state.drained == nil {
		state.drained = make(chan struct{})
	}
	wait := state.drained
	l.mu.Unlock()
	select {
	case <-ctx.Done():
		return failure(LifecycleDraining, ctx.Err())
	case <-wait:
	}
	l.mu.Lock()
	unknown := state.unknown
	l.mu.Unlock()
	if unknown {
		return failure(UnknownWriteOutcome, nil)
	}
	return nil
}

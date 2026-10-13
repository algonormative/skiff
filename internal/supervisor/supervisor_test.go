package supervisor

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/algonormative/skiff/internal/config"
	"github.com/algonormative/skiff/internal/logbuf"
	"github.com/algonormative/skiff/internal/status"
)

// --- Mock infrastructure ---

type mockCmd struct {
	mu        sync.Mutex
	pid       int
	started   bool
	startErr  error
	waitCh    chan error
	signals   []os.Signal
	groupSigs []syscall.Signal
	killed    bool
}

func newMockCmd(pid int) *mockCmd {
	return &mockCmd{pid: pid, waitCh: make(chan error, 1)}
}

func (c *mockCmd) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.startErr != nil {
		return c.startErr
	}
	c.started = true
	return nil
}

func (c *mockCmd) Started() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

func (c *mockCmd) setStartErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.startErr = err
}
func (c *mockCmd) Wait() error                { return <-c.waitCh }
func (c *mockCmd) Pid() int                   { return c.pid }
func (c *mockCmd) SweepGroup()                {}
func (c *mockCmd) SetDir(string)              {}
func (c *mockCmd) SetEnv([]string)            {}
func (c *mockCmd) SetStdout(io.Writer)        {}
func (c *mockCmd) SetStderr(io.Writer)        {}
func (c *mockCmd) Signal(sig os.Signal) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.signals = append(c.signals, sig)
	return nil
}
func (c *mockCmd) SignalGroup(sig syscall.Signal) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.groupSigs = append(c.groupSigs, sig)
	return nil
}
func (c *mockCmd) Kill() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.killed = true
	return nil
}

func (c *mockCmd) exit(err error) { c.waitCh <- err }

type mockCmdFactory struct {
	mu           sync.Mutex
	cmds         []*mockCmd
	nextPid      atomic.Int32
	cmdReady     chan *mockCmd
	nextStartErr error // if set, next created cmd will fail Start()
}

func newMockFactory() *mockCmdFactory {
	f := &mockCmdFactory{cmdReady: make(chan *mockCmd, 32)}
	f.nextPid.Store(1000)
	return f
}

func (f *mockCmdFactory) Command(_ context.Context, _ string, _ ...string) Cmd {
	pid := int(f.nextPid.Add(1))
	cmd := newMockCmd(pid)
	f.mu.Lock()
	if f.nextStartErr != nil {
		cmd.startErr = f.nextStartErr
		f.nextStartErr = nil
	}
	f.cmds = append(f.cmds, cmd)
	f.mu.Unlock()
	f.cmdReady <- cmd
	return cmd
}

func (f *mockCmdFactory) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.cmds)
}

// waitForCmd blocks until a new cmd is created or the context is cancelled.
func (f *mockCmdFactory) waitForCmd(t *testing.T, timeout time.Duration) *mockCmd {
	t.Helper()
	select {
	case cmd := <-f.cmdReady:
		return cmd
	case <-time.After(timeout):
		t.Fatal("timed out waiting for new command")
		return nil
	}
}

// --- Test helpers ---

func newTestSupervisor(factory CmdFactory) *Supervisor {
	state := status.NewSharedState()
	logs := logbuf.New(100)
	logger := newDiscardLogger()
	return New(state, logs, "", "", logger, factory)
}

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func baseCfg() config.ServiceConfig {
	return config.ServiceConfig{
		Command:            []string{"test-cmd"},
		RestartPolicy:      "never",
		RestartBackoffSecs: 1,
	}
}

// --- Tests ---

func TestStart_LaunchesProcess(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	if err := sup.Start(ctx, "svc1", cfg); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cmd := f.waitForCmd(t, 2*time.Second)
	// Give the goroutine time to call Start()
	time.Sleep(50 * time.Millisecond)
	if !cmd.Started() {
		t.Error("expected process to be started")
	}

	// Clean up: let process exit
	cmd.exit(nil)
	time.Sleep(50 * time.Millisecond)
}

func TestStart_AlreadyRunning(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	sup.Start(ctx, "svc1", cfg)
	f.waitForCmd(t, 2*time.Second)

	// Give the goroutine time to register the process
	time.Sleep(50 * time.Millisecond)

	err := sup.Start(ctx, "svc1", cfg)
	if err == nil {
		t.Fatal("expected error for already-running service")
	}

	// Clean up
	cancel()
}

func TestRestartPolicy_Never(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	cfg.RestartPolicy = "never"
	sup.Start(ctx, "svc1", cfg)

	cmd := f.waitForCmd(t, 2*time.Second)
	cmd.exit(fmt.Errorf("exit status 1"))

	// Wait for supervise loop to finish
	time.Sleep(100 * time.Millisecond)

	if f.count() != 1 {
		t.Errorf("expected 1 command (no restart), got %d", f.count())
	}
}

func TestRestartPolicy_Always(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	cfg.RestartPolicy = "always"
	cfg.MaxRestarts = 2
	sup.Start(ctx, "svc1", cfg)

	// First run
	cmd1 := f.waitForCmd(t, 2*time.Second)
	cmd1.exit(nil) // exit code 0 — should still restart with "always"

	// First restart
	cmd2 := f.waitForCmd(t, 3*time.Second)
	cmd2.exit(nil)

	// Second restart
	cmd3 := f.waitForCmd(t, 5*time.Second)
	cmd3.exit(nil)

	// Should stop now (restarts=2 >= max_restarts=2)
	time.Sleep(200 * time.Millisecond)
	if f.count() != 3 {
		t.Errorf("expected 3 commands (1 + 2 restarts), got %d", f.count())
	}
}

func TestRestartPolicy_OnFailure_ExitZero(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	cfg.RestartPolicy = "on-failure"
	sup.Start(ctx, "svc1", cfg)

	cmd := f.waitForCmd(t, 2*time.Second)
	cmd.exit(nil) // exit 0 — should NOT restart

	time.Sleep(100 * time.Millisecond)
	if f.count() != 1 {
		t.Errorf("expected 1 command (no restart on exit 0), got %d", f.count())
	}
}

func TestRestartPolicy_OnFailure_ExitNonZero(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	cfg.RestartPolicy = "on-failure"
	cfg.MaxRestarts = 1
	sup.Start(ctx, "svc1", cfg)

	// First run — fail
	cmd1 := f.waitForCmd(t, 2*time.Second)
	cmd1.exit(&exitError{code: 1})

	// Should restart once
	cmd2 := f.waitForCmd(t, 3*time.Second)
	cmd2.exit(&exitError{code: 1})

	// Max restarts hit
	time.Sleep(200 * time.Millisecond)
	if f.count() != 2 {
		t.Errorf("expected 2 commands (1 + 1 restart), got %d", f.count())
	}
}

func TestMaxRestarts_Enforced(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	cfg.RestartPolicy = "always"
	cfg.MaxRestarts = 1
	sup.Start(ctx, "svc1", cfg)

	cmd1 := f.waitForCmd(t, 2*time.Second)
	cmd1.exit(nil)

	cmd2 := f.waitForCmd(t, 3*time.Second)
	cmd2.exit(nil)

	// Should not get a third
	time.Sleep(200 * time.Millisecond)
	if f.count() != 2 {
		t.Errorf("expected 2 commands (max_restarts=1), got %d", f.count())
	}
}

func TestStop_SetsStopping(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	cfg.RestartPolicy = "always"
	sup.Start(ctx, "svc1", cfg)

	cmd := f.waitForCmd(t, 2*time.Second)
	time.Sleep(50 * time.Millisecond)

	// Stop is synchronous: it waits for the supervise goroutine to exit.
	// Unblock cmd.Wait() in the background so Stop can return.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cmd.exit(nil)
	}()
	sup.Stop("svc1")

	if sup.IsRunning("svc1") {
		t.Error("expected service to not be running after Stop")
	}

	sigs := cmd.groupSigs
	if len(sigs) == 0 {
		t.Error("expected SIGTERM to be sent to process group")
	} else if sigs[0] != syscall.SIGTERM {
		t.Errorf("expected SIGTERM, got %v", sigs[0])
	}
}

func TestKill_SendsKill(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	sup.Start(ctx, "svc1", cfg)

	cmd := f.waitForCmd(t, 2*time.Second)
	time.Sleep(50 * time.Millisecond)

	go func() {
		time.Sleep(20 * time.Millisecond)
		cmd.exit(nil)
	}()
	sup.Kill("svc1")

	if !cmd.killed {
		t.Error("expected Kill to be called on process")
	}
	if sup.IsRunning("svc1") {
		t.Error("expected service to not be running after Kill")
	}
}

func TestStopAll(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	cfg.RestartPolicy = "always"

	sup.Start(ctx, "svc1", cfg)
	sup.Start(ctx, "svc2", cfg)

	cmd1 := f.waitForCmd(t, 2*time.Second)
	cmd2 := f.waitForCmd(t, 2*time.Second)
	time.Sleep(50 * time.Millisecond)

	go func() {
		time.Sleep(20 * time.Millisecond)
		cmd1.exit(nil)
		cmd2.exit(nil)
	}()
	sup.StopAll()

	if sup.IsRunning("svc1") || sup.IsRunning("svc2") {
		t.Error("expected all services to be stopped")
	}
}

func TestIsRunning(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if sup.IsRunning("svc1") {
		t.Error("expected IsRunning=false before start")
	}

	cfg := baseCfg()
	sup.Start(ctx, "svc1", cfg)
	f.waitForCmd(t, 2*time.Second)
	time.Sleep(50 * time.Millisecond)

	if !sup.IsRunning("svc1") {
		t.Error("expected IsRunning=true after start")
	}
}

func TestPID(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if sup.PID("svc1") != 0 {
		t.Error("expected PID=0 before start")
	}

	cfg := baseCfg()
	sup.Start(ctx, "svc1", cfg)
	cmd := f.waitForCmd(t, 2*time.Second)
	time.Sleep(50 * time.Millisecond)

	pid := sup.PID("svc1")
	if pid != cmd.pid {
		t.Errorf("expected PID=%d, got %d", cmd.pid, pid)
	}

	// Clean up
	cancel()
	cmd.exit(nil)
}

func TestStop_NotRunning_Noop(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)

	err := sup.Stop("nonexistent")
	if err != nil {
		t.Errorf("expected nil error for stopping non-existent service, got %v", err)
	}
}

func TestKill_NotRunning_Noop(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)

	err := sup.Kill("nonexistent")
	if err != nil {
		t.Errorf("expected nil error for killing non-existent service, got %v", err)
	}
}

func TestContextCancel_StopsSupervise(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())

	cfg := baseCfg()
	cfg.RestartPolicy = "always"
	sup.Start(ctx, "svc1", cfg)

	cmd := f.waitForCmd(t, 2*time.Second)
	cancel() // cancel parent context
	cmd.exit(nil)

	time.Sleep(100 * time.Millisecond)

	// Should not have restarted
	if f.count() != 1 {
		t.Errorf("expected 1 command (context cancelled), got %d", f.count())
	}
}

func TestStartFailure_NoRestart(t *testing.T) {
	f := newMockFactory()
	sup := newTestSupervisor(f)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set the factory to make the next cmd fail on Start()
	f.mu.Lock()
	f.nextStartErr = fmt.Errorf("command not found")
	f.mu.Unlock()

	cfg := baseCfg()
	cfg.RestartPolicy = "always"
	sup.Start(ctx, "svc1", cfg)

	// Drain the cmdReady channel
	f.waitForCmd(t, 2*time.Second)
	time.Sleep(200 * time.Millisecond)

	// Should not have retried after start failure
	if f.count() != 1 {
		t.Errorf("expected 1 command (start failed, no retry), got %d", f.count())
	}
}

func TestState_TransitionsOnLifecycle(t *testing.T) {
	f := newMockFactory()
	state := status.NewSharedState()
	logs := logbuf.New(100)
	sup := New(state, logs, "", "", newDiscardLogger(), f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := baseCfg()
	cfg.RestartPolicy = "never"
	sup.Start(ctx, "svc1", cfg)

	f.waitForCmd(t, 2*time.Second)
	time.Sleep(50 * time.Millisecond)

	// Should be running
	rs, ok := state.GetResource("svc1")
	if !ok {
		t.Fatal("expected resource status for svc1")
	}
	if rs.State != status.StateRunning {
		t.Errorf("expected state Running, got %s", rs.State)
	}
}

// exitError implements error + ExitCode() to work with the supervisor's
// exitCoder interface check.
type exitError struct {
	code int
}

func (e *exitError) Error() string { return fmt.Sprintf("exit status %d", e.code) }
func (e *exitError) ExitCode() int { return e.code }

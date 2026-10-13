package supervisor

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/algonormative/skiff/internal/config"
	"github.com/algonormative/skiff/internal/logbuf"
	"github.com/algonormative/skiff/internal/status"
)

// stopWaitTimeout bounds how long a graceful Stop waits for the child to
// exit after SIGTERM before escalating to SIGKILL. Kept small so daemon
// shutdown stays responsive but large enough for Python interpreters etc.
const stopWaitTimeout = 5 * time.Second

// crashLoopWindow / crashLoopThreshold define when a service is considered
// to be in a crash loop (and thus its health should be reported unhealthy
// regardless of HTTP probe results — see split-brain detection).
const (
	crashLoopWindow    = 30 * time.Second
	crashLoopThreshold = 3
)

// Supervisor manages native services as child processes.
type Supervisor struct {
	mu        sync.Mutex
	processes map[string]*managedProcess
	dones     map[string]chan struct{} // per-service "supervise goroutine exited" signal
	state     *status.SharedState
	logs      *logbuf.LogBuffer
	logsDir   string
	logger    *slog.Logger
	factory   CmdFactory
	pids      *pidStore
}

type managedProcess struct {
	name        string
	cfg         config.ServiceConfig
	cmd         Cmd
	cancel      context.CancelFunc
	pid         int
	restarts    int
	startedAt   time.Time
	stopping    bool
	recentExits []time.Time // sliding window for crash-loop detection
}

// New creates a Supervisor. If factory is nil, uses real os/exec.
// pidFilePath enables orphan tracking — the supervisor records each
// spawned PID to that file so the next daemon process can clean up
// orphans left behind by an ungraceful shutdown. Pass "" to disable.
func New(state *status.SharedState, logs *logbuf.LogBuffer, logsDir, pidFilePath string, logger *slog.Logger, factory ...CmdFactory) *Supervisor {
	var f CmdFactory
	if len(factory) > 0 && factory[0] != nil {
		f = factory[0]
	} else {
		f = newExecCmdFactory()
	}
	return &Supervisor{
		processes: make(map[string]*managedProcess),
		dones:     make(map[string]chan struct{}),
		state:     state,
		logs:      logs,
		logsDir:   logsDir,
		logger:    logger,
		factory:   f,
		pids:      newPIDStore(pidFilePath),
	}
}

// Start launches a service as a child process with restart policy.
func (s *Supervisor) Start(ctx context.Context, name string, cfg config.ServiceConfig) error {
	s.mu.Lock()
	if existing, ok := s.dones[name]; ok {
		// A previous supervise goroutine is still alive (running or
		// finishing teardown). Refuse to spawn a duplicate.
		select {
		case <-existing:
			// Stale entry — clean it up and proceed.
			delete(s.dones, name)
		default:
			s.mu.Unlock()
			pid := 0
			if p, ok := s.processes[name]; ok {
				pid = p.pid
			}
			return fmt.Errorf("service %q is already running (pid %d)", name, pid)
		}
	}
	done := make(chan struct{})
	s.dones[name] = done
	s.mu.Unlock()

	go func() {
		defer close(done)
		s.supervise(ctx, name, cfg)
	}()
	return nil
}

func (s *Supervisor) supervise(parentCtx context.Context, name string, cfg config.ServiceConfig) {
	var restarts int
	backoff := time.Duration(cfg.RestartBackoffSecs) * time.Second
	if backoff == 0 {
		backoff = 5 * time.Second
	}
	currentBackoff := backoff

	for {
		ctx, cancel := context.WithCancel(parentCtx)

		s.state.SetResource(&status.ResourceStatus{
			Name:       name,
			Type:       status.TypeService,
			State:      status.StateStarting,
			ConfigHash: config.Hash(cfg),
			DependsOn:  cfg.DependsOn,
		})

		cmd := s.factory.Command(ctx, cfg.Command[0], cfg.Command[1:]...)
		cmd.SetDir(cfg.WorkingDir)
		cmd.SetEnv(buildEnv(cfg.Env))

		// Set up log file
		var logFile *os.File
		if cfg.LogFile != "" {
			logPath := cfg.LogFile
			if !filepath.IsAbs(logPath) {
				logPath = filepath.Join(s.logsDir, logPath)
			}
			var err error
			logFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				s.logger.Error("failed to open log file", "service", name, "error", err)
			} else {
				cmd.SetStdout(logFile)
				cmd.SetStderr(logFile)
			}
		}

		err := cmd.Start()
		if err != nil {
			s.logger.Error("failed to start service", "service", name, "error", err)
			s.state.SetResource(&status.ResourceStatus{
				Name:       name,
				Type:       status.TypeService,
				State:      status.StateFailed,
				LastError:  err.Error(),
				ConfigHash: config.Hash(cfg),
			})
			cancel()
			if logFile != nil {
				logFile.Close()
			}
			s.mu.Lock()
			delete(s.processes, name)
			s.mu.Unlock()
			return
		}

		now := time.Now()
		pid := cmd.Pid()
		s.logger.Info("service started", "service", name, "pid", pid)
		s.logs.Append(name, fmt.Sprintf("service started (pid %d)", pid))

		s.mu.Lock()
		var recentExits []time.Time
		if existing, ok := s.processes[name]; ok {
			recentExits = existing.recentExits
		}
		s.processes[name] = &managedProcess{
			name:        name,
			cfg:         cfg,
			cmd:         cmd,
			cancel:      cancel,
			pid:         pid,
			restarts:    restarts,
			startedAt:   now,
			recentExits: recentExits,
		}
		s.mu.Unlock()

		// Persist PID for orphan reaping by the next daemon if we crash.
		if s.pids != nil {
			pgid, _ := syscall.Getpgid(pid)
			argv0 := ""
			if len(cfg.Command) > 0 {
				argv0 = cfg.Command[0]
			}
			_ = s.pids.Set(pidRecord{
				Name:      name,
				PID:       pid,
				PGID:      pgid,
				Command:   argv0,
				StartedAt: now,
			})
		}

		s.state.SetResource(&status.ResourceStatus{
			Name:       name,
			Type:       status.TypeService,
			State:      status.StateRunning,
			PID:        pid,
			StartedAt:  now,
			ConfigHash: config.Hash(cfg),
			DependsOn:  cfg.DependsOn,
		})

		// Wait for process to exit
		waitErr := cmd.Wait()
		if logFile != nil {
			logFile.Close()
		}

		s.mu.Lock()
		proc := s.processes[name]
		isStopping := proc != nil && proc.stopping
		s.mu.Unlock()

		// Sweep the process group for stragglers. Wrappers like `uv run X`
		// fork a child that inherits the wrapper's pgid — when the wrapper
		// exits (or is SIGTERM'd), the child gets re-parented to launchd
		// but the pgid lingers as long as the child is alive. With
		// Setpgid:true the leader's pgid==pid, so we can use pid here even
		// after the leader is gone, and the group is this service's alone.
		// Sweep on every exit, not only on Stop: a context cancel (daemon
		// shutdown) or a crashed wrapper leaves the same stragglers, and
		// the PID record is dropped below, so nothing else would reap them.
		cmd.SweepGroup()

		// Drop PID record now that the process is gone — keeps the file
		// from accumulating stale entries while the supervise loop iterates.
		if s.pids != nil {
			_ = s.pids.Remove(name)
		}

		// Check if we were asked to stop
		if isStopping || parentCtx.Err() != nil {
			s.state.SetResource(&status.ResourceStatus{
				Name:       name,
				Type:       status.TypeService,
				State:      status.StateStopped,
				ConfigHash: config.Hash(cfg),
			})
			cancel()
			s.mu.Lock()
			delete(s.processes, name)
			s.mu.Unlock()
			return
		}

		exitCode := 0
		if waitErr != nil {
			type exitCoder interface{ ExitCode() int }
			if ec, ok := waitErr.(exitCoder); ok {
				exitCode = ec.ExitCode()
			} else {
				exitCode = 1 // non-zero for unknown errors
			}
		}

		s.logger.Warn("service exited", "service", name, "exit_code", exitCode, "restarts", restarts)
		s.logs.Append(name, fmt.Sprintf("service exited (code %d, restart %d)", exitCode, restarts))

		// Track recent exits for crash-loop / split-brain detection.
		s.recordExit(name)

		// Check restart policy
		shouldRestart := false
		switch cfg.RestartPolicy {
		case "always":
			shouldRestart = true
		case "on-failure":
			shouldRestart = exitCode != 0
		case "never":
			shouldRestart = false
		}

		if cfg.MaxRestarts > 0 && restarts >= cfg.MaxRestarts {
			shouldRestart = false
			s.logger.Error("service exceeded max restarts", "service", name, "max", cfg.MaxRestarts)
		}

		if !shouldRestart {
			s.state.SetResource(&status.ResourceStatus{
				Name:       name,
				Type:       status.TypeService,
				State:      status.StateStopped,
				ExitCode:   exitCode,
				ConfigHash: config.Hash(cfg),
			})
			cancel()
			s.mu.Lock()
			delete(s.processes, name)
			s.mu.Unlock()
			return
		}

		restarts++

		// Mark health unhealthy if we are in a crash loop. This is the
		// split-brain guard: even if an orphaned predecessor is still
		// answering on the bound port and making the HTTP probe pass,
		// our own supervised process is failing to come up — so report
		// unhealthy from the supervisor side.
		if s.inCrashLoop(name) {
			s.markCrashLoopUnhealthy(name, exitCode, restarts)
		} else {
			s.state.SetResource(&status.ResourceStatus{
				Name:       name,
				Type:       status.TypeService,
				State:      status.StateStarting,
				LastError:  fmt.Sprintf("restarting (attempt %d, backoff %s)", restarts, currentBackoff),
				ConfigHash: config.Hash(cfg),
			})
		}

		// Backoff before restart
		select {
		case <-time.After(currentBackoff):
		case <-parentCtx.Done():
			cancel()
			s.mu.Lock()
			delete(s.processes, name)
			s.mu.Unlock()
			return
		}

		// Exponential backoff, cap at 60s
		currentBackoff *= 2
		if currentBackoff > 60*time.Second {
			currentBackoff = 60 * time.Second
		}

		cancel()
	}
}

// recordExit appends the current time to the recent-exits sliding window
// and trims entries older than crashLoopWindow.
func (s *Supervisor) recordExit(name string) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	proc, ok := s.processes[name]
	if !ok {
		return
	}
	proc.recentExits = append(proc.recentExits, now)
	cutoff := now.Add(-crashLoopWindow)
	trimmed := proc.recentExits[:0]
	for _, t := range proc.recentExits {
		if t.After(cutoff) {
			trimmed = append(trimmed, t)
		}
	}
	proc.recentExits = trimmed
}

// inCrashLoop reports whether the named service has exited at least
// crashLoopThreshold times within the last crashLoopWindow.
func (s *Supervisor) inCrashLoop(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	proc, ok := s.processes[name]
	if !ok {
		return false
	}
	return len(proc.recentExits) >= crashLoopThreshold
}

// markCrashLoopUnhealthy publishes an unhealthy HealthState so that the
// API and `skiff ps` show the supervised process as unhealthy even if the
// HTTP probe is being satisfied by an orphan from a previous daemon
// holding the listening port.
func (s *Supervisor) markCrashLoopUnhealthy(name string, exitCode, attempt int) {
	rs, ok := s.state.GetResource(name)
	if !ok {
		return
	}
	if rs.Health == nil {
		rs.Health = &status.HealthState{}
	}
	rs.Health.Status = "unhealthy"
	rs.Health.LastCheck = time.Now()
	rs.Health.LastError = fmt.Sprintf("crash loop: %d exits in %s (last exit code %d, attempt %d)",
		crashLoopThreshold, crashLoopWindow, exitCode, attempt)
	rs.State = status.StateStarting
	s.state.SetResource(rs)
}

// Stop sends SIGTERM to a service process and waits for it to exit
// (escalating to SIGKILL after stopWaitTimeout). Synchronous so that
// callers — daemon shutdown, OnUnhealthy auto-restart, the apply
// reconciler — can rely on the port being released before they spawn
// a replacement. Returns nil if the service was not running.
func (s *Supervisor) Stop(name string) error {
	s.mu.Lock()
	proc, exists := s.processes[name]
	done := s.dones[name]
	if !exists || proc.cmd == nil {
		s.mu.Unlock()
		// No live process; if a placeholder done is hanging around
		// (Start was called but supervise hasn't registered yet),
		// nothing to do — Start path itself owns that lifecycle.
		return nil
	}
	proc.stopping = true
	cmd := proc.cmd
	cancel := proc.cancel
	pid := proc.pid
	s.mu.Unlock()

	s.logger.Info("stopping service", "service", name, "pid", pid)

	_ = cmd.SignalGroup(syscall.SIGTERM)

	// Cancel the supervise context so the restart-backoff sleep wakes
	// up immediately if we caught the supervise loop between exits.
	if cancel != nil {
		cancel()
	}

	if done != nil {
		select {
		case <-done:
			// Supervise goroutine exited cleanly.
		case <-time.After(stopWaitTimeout):
			s.logger.Warn("graceful stop timed out, sending SIGKILL", "service", name, "pid", pid)
			_ = cmd.Kill()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				s.logger.Error("process did not exit after SIGKILL", "service", name, "pid", pid)
			}
		}
	}

	s.mu.Lock()
	delete(s.processes, name)
	delete(s.dones, name)
	s.mu.Unlock()

	if s.pids != nil {
		_ = s.pids.Remove(name)
	}

	s.logs.Append(name, "service stopped")
	return nil
}

// Kill sends SIGKILL to a service process and waits for the supervise
// goroutine to exit.
func (s *Supervisor) Kill(name string) error {
	s.mu.Lock()
	proc, exists := s.processes[name]
	done := s.dones[name]
	if !exists || proc.cmd == nil {
		s.mu.Unlock()
		return nil
	}
	proc.stopping = true
	cmd := proc.cmd
	cancel := proc.cancel
	s.mu.Unlock()

	_ = cmd.Kill()
	if cancel != nil {
		cancel()
	}

	if done != nil {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}

	s.mu.Lock()
	delete(s.processes, name)
	delete(s.dones, name)
	s.mu.Unlock()

	if s.pids != nil {
		_ = s.pids.Remove(name)
	}

	return nil
}

// StopAll gracefully stops all managed processes in parallel.
func (s *Supervisor) StopAll() {
	s.mu.Lock()
	names := make([]string, 0, len(s.processes))
	for name := range s.processes {
		names = append(names, name)
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, name := range names {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			_ = s.Stop(n)
		}(name)
	}
	wg.Wait()
}

// IsRunning checks if a service is currently managed and running.
func (s *Supervisor) IsRunning(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	proc, exists := s.processes[name]
	return exists && proc.cmd != nil && proc.pid != 0 && !proc.stopping
}

// PID returns the PID of a managed process, or 0 if not running.
func (s *Supervisor) PID(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if proc, exists := s.processes[name]; exists {
		return proc.pid
	}
	return 0
}

func buildEnv(env map[string]string) []string {
	result := os.Environ()
	for k, v := range env {
		result = append(result, fmt.Sprintf("%s=%s", k, v))
	}
	return result
}

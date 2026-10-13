package supervisor

import (
	"context"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// Cmd abstracts a subprocess for testability.
type Cmd interface {
	Start() error
	Wait() error
	Pid() int
	Signal(sig os.Signal) error
	SignalGroup(sig syscall.Signal) error
	// SweepGroup terminates what is left of the process group after the
	// leader exited (children of a wrapper such as `uv run X`).
	SweepGroup()
	Kill() error
	SetDir(dir string)
	SetEnv(env []string)
	SetStdout(w io.Writer)
	SetStderr(w io.Writer)
}

// CmdFactory creates Cmd instances.
type CmdFactory interface {
	Command(ctx context.Context, name string, args ...string) Cmd
}

// execCmdFactory is the real implementation using os/exec.
type execCmdFactory struct{}

func newExecCmdFactory() CmdFactory {
	return &execCmdFactory{}
}

func (f *execCmdFactory) Command(ctx context.Context, name string, args ...string) Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// On context cancel (daemon shutdown) signal the whole process group, not
	// just the leader: a wrapper such as `uv run X` would otherwise die alone
	// and leave X holding the service's port. WaitDelay bounds the wait for a
	// group that ignores SIGTERM; the supervise loop then SIGKILLs it.
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	}
	cmd.WaitDelay = 5 * time.Second
	return &execCmd{cmd: cmd}
}

// execCmd wraps *exec.Cmd to implement the Cmd interface.
type execCmd struct {
	cmd *exec.Cmd
}

func (c *execCmd) Start() error            { return c.cmd.Start() }
func (c *execCmd) Wait() error             { return c.cmd.Wait() }
func (c *execCmd) SetDir(dir string)       { c.cmd.Dir = dir }
func (c *execCmd) SetEnv(env []string)     { c.cmd.Env = env }
func (c *execCmd) SetStdout(w io.Writer)   { c.cmd.Stdout = w }
func (c *execCmd) SetStderr(w io.Writer)   { c.cmd.Stderr = w }

func (c *execCmd) Pid() int {
	if c.cmd.Process != nil {
		return c.cmd.Process.Pid
	}
	return 0
}

func (c *execCmd) Signal(sig os.Signal) error {
	if c.cmd.Process == nil {
		return nil
	}
	return c.cmd.Process.Signal(sig)
}

func (c *execCmd) SignalGroup(sig syscall.Signal) error {
	if c.cmd.Process == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(c.cmd.Process.Pid)
	if err != nil {
		return c.cmd.Process.Signal(sig)
	}
	return syscall.Kill(-pgid, sig)
}

func (c *execCmd) Kill() error {
	if c.cmd.Process == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(c.cmd.Process.Pid)
	if err != nil {
		return c.cmd.Process.Kill()
	}
	return syscall.Kill(-pgid, syscall.SIGKILL)
}

// SweepGroup terminates whatever is left of the process group after its leader
// exited: SIGTERM, up to 2 s for a clean exit, then SIGKILL. With Setpgid the
// group id is the leader's pid, so it still names this group once the leader
// is gone, and only this service's processes are in it.
func (c *execCmd) SweepGroup() {
	if c.cmd.Process == nil || c.cmd.Process.Pid <= 1 {
		return
	}
	pgid := c.cmd.Process.Pid
	if syscall.Kill(-pgid, syscall.Signal(0)) != nil {
		return // group is empty
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(-pgid, syscall.Signal(0)) != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
}

package supervisor

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/algonormative/skiff/internal/config"
)

// A wrapper (like `uv run X`) that dies leaves its child in the service's
// process group, still holding the service's port. When the supervise context
// is cancelled (daemon shutdown) exec.CommandContext kills only the wrapper,
// so the group must be swept or the child outlives the daemon as an orphan.
func TestContextCancelReapsWrapperChild(t *testing.T) {
	childPIDFile := filepath.Join(t.TempDir(), "child.pid")
	sup := newTestSupervisor(newExecCmdFactory())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.ServiceConfig{
		Command:       []string{"sh", "-c", "sleep 300 & echo $! > " + childPIDFile + "; wait"},
		RestartPolicy: "never",
	}
	if err := sup.Start(ctx, "wrapped", cfg); err != nil {
		t.Fatal(err)
	}

	var child int
	for i := 0; i < 100 && child == 0; i++ {
		time.Sleep(20 * time.Millisecond)
		if b, err := os.ReadFile(childPIDFile); err == nil {
			child, _ = strconv.Atoi(strings.TrimSpace(string(b)))
		}
	}
	if child == 0 {
		t.Fatal("wrapper never started its child")
	}
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })

	cancel()

	for i := 0; i < 200; i++ {
		if syscall.Kill(child, 0) != nil {
			return // reaped
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("child %d of the wrapper survived the supervise context being cancelled", child)
}

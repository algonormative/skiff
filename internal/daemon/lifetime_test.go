package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/algonormative/skiff/internal/config"
)

// serveAndCancel runs a request the way net/http does: the request context is
// cancelled once the handler returns.
func serveAndCancel(mux *http.ServeMux, method, path string, body interface{}) *httptest.ResponseRecorder {
	data, _ := json.Marshal(body)
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(method, path, bytes.NewBuffer(data)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	cancel()
	return rr
}

func lifetimeDaemon(t *testing.T) *Daemon {
	t.Helper()
	cfg := &config.Config{
		Version: 1,
		Paths:   config.PathsConfig{Base: t.TempDir(), Logs: t.TempDir()},
		Daemon:  config.DaemonConfig{LogBufferLines: 100, StatusPollIntervalSecs: 5, ShutdownTimeoutSecs: 5},
		Services: map[string]config.ServiceConfig{
			"svc": {
				Command:       []string{"sleep", "30"},
				RestartPolicy: "always",
				HealthCheck:   &config.HealthCheckConfig{Type: "command", Command: []string{"true"}, IntervalSecs: 1, FailureThreshold: 1},
			},
		},
	}
	d, _ := newTestDaemon(cfg)
	runCtx, cancel := context.WithCancel(context.Background())
	d.runCtx = runCtx
	t.Cleanup(func() {
		d.health.StopAll()
		d.supervisor.StopAll()
		cancel()
	})
	return d
}

// A service (and its health probe) started through the API must outlive the
// HTTP request that started it: they belong to the daemon, not the request.
func assertOutlivesRequest(t *testing.T, d *Daemon) {
	t.Helper()
	pid := waitPID(t, d)
	time.Sleep(1500 * time.Millisecond)
	if !d.supervisor.IsRunning("svc") || d.supervisor.PID("svc") != pid {
		t.Fatalf("service did not survive the end of the request (pid before %d, after %d, running=%v)",
			pid, d.supervisor.PID("svc"), d.supervisor.IsRunning("svc"))
	}
	// The probe ticks every second; allow slack for a loaded or -race run.
	for i := 0; i < 100; i++ {
		if rs, ok := d.state.GetResource("svc"); ok && rs.Health != nil && !rs.Health.LastCheck.IsZero() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	rs, _ := d.state.GetResource("svc")
	t.Fatalf("health probe stopped with the request: %+v", rs)
}

// waitPID waits for the supervise goroutine to spawn the service.
func waitPID(t *testing.T, d *Daemon) int {
	t.Helper()
	for i := 0; i < 50; i++ {
		if pid := d.supervisor.PID("svc"); pid != 0 {
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	rs, _ := d.state.GetResource("svc")
	t.Fatalf("service never started: %+v logs=%v", rs, d.logs.Lines("svc", 10, ""))
	return 0
}

func TestUpServiceOutlivesRequest(t *testing.T) {
	d := lifetimeDaemon(t)
	rr := serveAndCancel(d.setupRoutes(), "POST", "/v1/up", map[string]interface{}{"names": []string{"svc"}})
	if rr.Code != http.StatusOK {
		t.Fatalf("up: %d %s", rr.Code, rr.Body.String())
	}
	assertOutlivesRequest(t, d)
}

func TestApplyNewServiceOutlivesRequest(t *testing.T) {
	d := lifetimeDaemon(t)
	rr := serveAndCancel(d.setupRoutes(), "POST", "/v1/apply", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("apply: %d %s", rr.Code, rr.Body.String())
	}
	assertOutlivesRequest(t, d)
}

func TestRestartServiceOutlivesRequest(t *testing.T) {
	d := lifetimeDaemon(t)
	if err := d.supervisor.Start(d.runCtx, "svc", d.cfg.Services["svc"]); err != nil {
		t.Fatal(err)
	}
	waitPID(t, d)
	rr := serveAndCancel(d.setupRoutes(), "POST", "/v1/restart/svc", nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("restart: %d %s", rr.Code, rr.Body.String())
	}
	waitPID(t, d)
	time.Sleep(1500 * time.Millisecond)
	if !d.supervisor.IsRunning("svc") {
		t.Fatal("restarted service did not survive the end of the request")
	}
}

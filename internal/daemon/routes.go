package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/algonormative/skiff/internal/config"
	"github.com/algonormative/skiff/internal/status"
)

func (d *Daemon) setupRoutes() *http.ServeMux {
	mux := http.NewServeMux()

	// Status routes
	mux.HandleFunc("GET /v1/status", d.handleStatus)
	mux.HandleFunc("GET /v1/status/{name}", d.handleStatusByName)
	mux.HandleFunc("GET /v1/services", d.handleServices)
	mux.HandleFunc("GET /v1/containers", d.handleContainers)
	mux.HandleFunc("GET /v1/schedules", d.handleSchedules)
	mux.HandleFunc("GET /v1/schedule/{name}", d.handleScheduleByName)

	// Replica routes
	mux.HandleFunc("GET /v1/replicas", d.handleReplicas)
	mux.HandleFunc("POST /v1/scale", d.handleScale)

	// Control routes
	mux.HandleFunc("POST /v1/up", d.handleUp)
	mux.HandleFunc("POST /v1/down", d.handleDown)
	mux.HandleFunc("POST /v1/apply", d.handleApply)
	mux.HandleFunc("POST /v1/restart/{name}", d.handleRestart)
	mux.HandleFunc("POST /v1/run", d.handleRun)
	mux.HandleFunc("POST /v1/build", d.handleBuild)
	mux.HandleFunc("POST /v1/exec/{name}", d.handleExec)
	mux.HandleFunc("POST /v1/schedule/{name}/run-now", d.handleRunNow)

	// Ad-hoc container routes
	mux.HandleFunc("POST /v1/containers/run", d.handleContainerRun)
	mux.HandleFunc("POST /v1/containers/{name}/stop", d.handleContainerStop)

	// Logs
	mux.HandleFunc("GET /v1/logs/{name}", d.handleLogs)

	// Stats
	mux.HandleFunc("GET /v1/stats", d.handleStats)
	mux.HandleFunc("GET /v1/stats/{name}", d.handleStatsByName)

	// Health
	mux.HandleFunc("GET /v1/health", d.handleHealth)

	return mux
}

func (d *Daemon) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (d *Daemon) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, d.state.Snapshot())
}

func (d *Daemon) handleStatusByName(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if rs, ok := d.state.GetResource(name); ok {
		writeJSON(w, http.StatusOK, rs)
		return
	}
	if ss, ok := d.state.GetSchedule(name); ok {
		writeJSON(w, http.StatusOK, ss)
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("resource %q not found", name)})
}

func (d *Daemon) handleServices(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, d.state.ResourcesByType(status.TypeService))
}

func (d *Daemon) handleContainers(w http.ResponseWriter, r *http.Request) {
	parent := r.URL.Query().Get("parent")
	if parent != "" {
		writeJSON(w, http.StatusOK, d.state.ResourcesByLabel("skiff.parent", parent))
		return
	}
	writeJSON(w, http.StatusOK, d.state.ResourcesByType(status.TypeContainer))
}

func (d *Daemon) handleSchedules(w http.ResponseWriter, r *http.Request) {
	snapshot := d.state.Snapshot()
	writeJSON(w, http.StatusOK, snapshot["schedules"])
}

func (d *Daemon) handleScheduleByName(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if ss, ok := d.state.GetSchedule(name); ok {
		writeJSON(w, http.StatusOK, ss)
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("schedule %q not found", name)})
}

func (d *Daemon) handleUp(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Names []string `json:"names"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}

	ctx := r.Context()
	started := []string{}
	errors := map[string]string{}

	names := d.resolveNames(body.Names)
	if len(names) == 0 {
		// Start all
		order, err := config.DependencyOrder(d.cfg)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		names = order
	}

	for _, name := range names {
		if svcCfg, ok := d.cfg.Services[name]; ok {
			if d.supervisor.IsRunning(name) {
				// Check config hash for changes
				rs, _ := d.state.GetResource(name)
				newHash := config.Hash(svcCfg)
				if rs != nil && rs.ConfigHash == newHash {
					continue // unchanged, skip
				}
				// Config changed, restart
				_ = d.supervisor.Stop(name)
			}
			if err := d.supervisor.Start(d.lifetimeCtx(), name, svcCfg); err != nil {
				errors[name] = err.Error()
			} else {
				started = append(started, name)
				if svcCfg.HealthCheck != nil {
					d.health.StartProbe(d.lifetimeCtx(), name, svcCfg.HealthCheck)
				}
			}
		} else if cCfg, ok := d.cfg.Containers[name]; ok {
			rs, _ := d.state.GetResource(name)
			newHash := config.Hash(cCfg)
			if rs != nil && rs.State == status.StateRunning && rs.ConfigHash == newHash {
				continue // unchanged
			}
			// Always stop/remove before re-running, regardless of state.
			// Handles running containers that need config updates and
			// leftover stopped/failed containers from previous daemon runs.
			_ = d.runtime.Stop(ctx, name)
			rtCfg := containerToRuntimeConfig(name, cCfg)
			if err := d.runtime.Run(ctx, name, rtCfg); err != nil {
				errors[name] = err.Error()
			} else {
				started = append(started, name)
				d.state.SetResource(&status.ResourceStatus{
					Name:       name,
					Type:       status.TypeContainer,
					State:      status.StateRunning,
					StartedAt:  time.Now(),
					ConfigHash: newHash,
					Ports:      cCfg.Ports,
					DependsOn:  cCfg.DependsOn,
				})
				if cCfg.HealthCheck != nil {
					d.health.StartProbe(d.lifetimeCtx(), name, cCfg.HealthCheck)
				}
			}
		} else {
			errors[name] = "not found in config"
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"started": started,
		"errors":  errors,
	})
}

func (d *Daemon) handleDown(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Names   []string `json:"names"`
		Volumes bool     `json:"volumes"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}

	ctx := r.Context()
	stopped := []string{}
	errors := map[string]string{}

	names := d.resolveNames(body.Names)
	if len(names) == 0 {
		// Stop all (reverse dependency order)
		order, err := config.DependencyOrder(d.cfg)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		// Reverse order
		for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
			order[i], order[j] = order[j], order[i]
		}
		names = order
	}

	for _, name := range names {
		d.health.StopProbe(name)

		if _, ok := d.cfg.Services[name]; ok {
			if err := d.supervisor.Stop(name); err != nil {
				errors[name] = err.Error()
			} else {
				stopped = append(stopped, name)
				d.state.SetResource(&status.ResourceStatus{
					Name:  name,
					Type:  status.TypeService,
					State: status.StateStopped,
				})
			}
		} else if _, ok := d.cfg.Containers[name]; ok {
			if err := d.runtime.Stop(ctx, name); err != nil {
				errors[name] = err.Error()
			} else {
				stopped = append(stopped, name)
				d.state.SetResource(&status.ResourceStatus{
					Name:  name,
					Type:  status.TypeContainer,
					State: status.StateStopped,
				})
			}
		}

		// Stop any ad-hoc children of this resource
		d.adhoc.StopChildren(name)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"stopped": stopped,
		"errors":  errors,
	})
}

func (d *Daemon) handleApply(w http.ResponseWriter, r *http.Request) {
	dryRun := r.URL.Query().Get("dry_run") == "true"

	type action struct {
		Resource string `json:"resource"`
		Action   string `json:"action"`
		Reason   string `json:"reason"`
	}

	var actions []action

	// Hot-reload config from disk so apply picks up YAML edits and
	// new values in the sibling .env file. Without this the in-memory
	// cfg from daemon startup is authoritative and apply is a no-op
	// for any change other than restart.
	cfg := d.cfg
	if d.configPath != "" {
		newCfg, reloadErr := config.Load(d.configPath)
		if reloadErr != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{
				"error": fmt.Sprintf("reload config: %s", reloadErr),
			})
			return
		}
		if !dryRun {
			d.cfgMu.Lock()
			d.cfg = newCfg
			d.cfgMu.Unlock()
		}
		cfg = newCfg
	}

	// Compute what needs to change
	allConfigNames := map[string]bool{}
	for name := range cfg.Services {
		allConfigNames[name] = true
	}
	for name := range cfg.Containers {
		allConfigNames[name] = true
	}

	// Check running resources
	snapshot := d.state.Snapshot()
	if resources, ok := snapshot["resources"].([]*status.ResourceStatus); ok {
		for _, rs := range resources {
			if !allConfigNames[rs.Name] {
				actions = append(actions, action{Resource: rs.Name, Action: "stop", Reason: "removed from config"})
			}
		}
	}

	for name, svcCfg := range cfg.Services {
		rs, ok := d.state.GetResource(name)
		newHash := config.Hash(svcCfg)
		if !ok || rs.State == status.StateStopped {
			actions = append(actions, action{Resource: name, Action: "start", Reason: "new in config"})
		} else if rs.ConfigHash != newHash {
			actions = append(actions, action{Resource: name, Action: "restart", Reason: "config changed"})
		} else {
			actions = append(actions, action{Resource: name, Action: "(none)", Reason: "unchanged"})
		}
	}
	for name, cCfg := range cfg.Containers {
		rs, ok := d.state.GetResource(name)
		newHash := config.Hash(cCfg)
		if !ok || rs.State == status.StateStopped {
			actions = append(actions, action{Resource: name, Action: "start", Reason: "new in config"})
		} else if rs.ConfigHash != newHash {
			actions = append(actions, action{Resource: name, Action: "restart", Reason: "config changed"})
		} else {
			actions = append(actions, action{Resource: name, Action: "(none)", Reason: "unchanged"})
		}
	}

	if dryRun {
		writeJSON(w, http.StatusOK, map[string]interface{}{"actions": actions})
		return
	}

	// Execute actions
	ctx := r.Context()
	for _, a := range actions {
		switch a.Action {
		case "stop":
			d.health.StopProbe(a.Resource)
			if d.supervisor.IsRunning(a.Resource) {
				_ = d.supervisor.Stop(a.Resource)
			} else {
				_ = d.runtime.Stop(ctx, a.Resource)
			}
			d.state.RemoveResource(a.Resource)
		case "start":
			if svcCfg, ok := cfg.Services[a.Resource]; ok {
				_ = d.supervisor.Start(d.lifetimeCtx(), a.Resource, svcCfg)
				if svcCfg.HealthCheck != nil {
					d.health.StartProbe(d.lifetimeCtx(), a.Resource, svcCfg.HealthCheck)
				}
			} else if cCfg, ok := cfg.Containers[a.Resource]; ok {
				rtCfg := containerToRuntimeConfig(a.Resource, cCfg)
				_ = d.runtime.Run(ctx, a.Resource, rtCfg)
				d.state.SetResource(&status.ResourceStatus{
					Name:       a.Resource,
					Type:       status.TypeContainer,
					State:      status.StateRunning,
					StartedAt:  time.Now(),
					ConfigHash: config.Hash(cCfg),
					Ports:      cCfg.Ports,
					DependsOn:  cCfg.DependsOn,
				})
				if cCfg.HealthCheck != nil {
					d.health.StartProbe(d.lifetimeCtx(), a.Resource, cCfg.HealthCheck)
				}
			}
		case "restart":
			if svcCfg, ok := cfg.Services[a.Resource]; ok {
				_ = d.supervisor.Stop(a.Resource)
				_ = d.supervisor.Start(d.lifetimeCtx(), a.Resource, svcCfg)
				if svcCfg.HealthCheck != nil {
					d.health.StartProbe(d.lifetimeCtx(), a.Resource, svcCfg.HealthCheck)
				}
			} else if cCfg, ok := cfg.Containers[a.Resource]; ok {
				_ = d.runtime.Stop(ctx, a.Resource)
				rtCfg := containerToRuntimeConfig(a.Resource, cCfg)
				_ = d.runtime.Run(ctx, a.Resource, rtCfg)
				d.state.SetResource(&status.ResourceStatus{
					Name:       a.Resource,
					Type:       status.TypeContainer,
					State:      status.StateRunning,
					StartedAt:  time.Now(),
					ConfigHash: config.Hash(cCfg),
					Ports:      cCfg.Ports,
					DependsOn:  cCfg.DependsOn,
				})
				if cCfg.HealthCheck != nil {
					d.health.StartProbe(d.lifetimeCtx(), a.Resource, cCfg.HealthCheck)
				}
			}
		}
	}

	// Reconcile schedules from the freshly loaded config.
	started, restarted, stopped := d.scheduler.Reconcile(d.lifetimeCtx(), cfg.Schedules)
	for _, n := range started {
		actions = append(actions, action{Resource: n, Action: "start", Reason: "new schedule"})
	}
	for _, n := range restarted {
		actions = append(actions, action{Resource: n, Action: "restart", Reason: "schedule changed"})
	}
	for _, n := range stopped {
		actions = append(actions, action{Resource: n, Action: "stop", Reason: "removed from config"})
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"actions": actions})
}

func (d *Daemon) handleRestart(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	// Check if this is a replica template name
	resolved := d.resolveNames([]string{name})
	if len(resolved) > 1 {
		// Template name — restart all members
		restarted := []string{}
		errors := map[string]string{}
		for _, member := range resolved {
			if cCfg, ok := d.cfg.Containers[member]; ok {
				_ = d.runtime.Stop(ctx, member)
				rtCfg := containerToRuntimeConfig(member, cCfg)
				if err := d.runtime.Run(ctx, member, rtCfg); err != nil {
					errors[member] = err.Error()
				} else {
					restarted = append(restarted, member)
					d.state.SetResource(&status.ResourceStatus{
						Name:       member,
						Type:       status.TypeContainer,
						State:      status.StateRunning,
						StartedAt:  time.Now(),
						ConfigHash: config.Hash(cCfg),
						Ports:      cCfg.Ports,
					})
				}
			}
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"restarted": restarted,
			"errors":    errors,
		})
		return
	}

	if svcCfg, ok := d.cfg.Services[name]; ok {
		_ = d.supervisor.Stop(name)
		if err := d.supervisor.Start(d.lifetimeCtx(), name, svcCfg); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"restarted": name})
		return
	}
	if cCfg, ok := d.cfg.Containers[name]; ok {
		_ = d.runtime.Stop(ctx, name)
		rtCfg := containerToRuntimeConfig(name, cCfg)
		if err := d.runtime.Run(ctx, name, rtCfg); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		d.state.SetResource(&status.ResourceStatus{
			Name:       name,
			Type:       status.TypeContainer,
			State:      status.StateRunning,
			StartedAt:  time.Now(),
			ConfigHash: config.Hash(cCfg),
			Ports:      cCfg.Ports,
		})
		writeJSON(w, http.StatusOK, map[string]string{"restarted": name})
		return
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("resource %q not found", name)})
}

func (d *Daemon) handleRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string   `json:"name"`
		Args []string `json:"args"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	cCfg, ok := d.cfg.Containers[body.Name]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("container %q not found", body.Name)})
		return
	}

	rtCfg := containerToRuntimeConfig(body.Name, cCfg)
	ephemeralName := fmt.Sprintf("%s-run-%d", body.Name, time.Now().Unix())

	if err := d.runtime.Run(r.Context(), ephemeralName, rtCfg); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"name": ephemeralName, "status": "started"})
}

func (d *Daemon) handleBuild(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Names []string `json:"names"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}

	ctx := r.Context()
	built := []string{}
	errors := map[string]string{}

	names := body.Names
	if len(names) == 0 {
		for name := range d.cfg.Containers {
			if d.cfg.Containers[name].Dockerfile != "" {
				names = append(names, name)
			}
		}
	}

	for _, name := range names {
		cCfg, ok := d.cfg.Containers[name]
		if !ok {
			errors[name] = "not found in config"
			continue
		}
		if cCfg.Dockerfile == "" {
			errors[name] = "no dockerfile specified"
			continue
		}
		rtCfg := containerToRuntimeConfig(name, cCfg)
		if err := d.runtime.Build(ctx, name, rtCfg); err != nil {
			errors[name] = err.Error()
		} else {
			built = append(built, name)
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"built":  built,
		"errors": errors,
	})
}

func (d *Daemon) handleExec(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	_, inConfig := d.cfg.Containers[name]
	_, inState := d.state.GetResource(name)
	if !inConfig && !inState {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("container %q not found", name)})
		return
	}

	var body struct {
		Command []string `json:"command"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	output, err := d.runtime.Exec(r.Context(), name, body.Command)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error":  err.Error(),
			"output": string(output),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"output": string(output),
	})
}

func (d *Daemon) handleRunNow(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := d.scheduler.TriggerNow(name); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"triggered": name})
}

func (d *Daemon) handleLogs(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	level := r.URL.Query().Get("level")
	source := r.URL.Query().Get("source")

	n := 100
	if nStr := r.URL.Query().Get("lines"); nStr != "" {
		fmt.Sscanf(nStr, "%d", &n)
	}

	// Validate name exists (in config or as ad-hoc container in state)
	_, svcOk := d.cfg.Services[name]
	_, cOk := d.cfg.Containers[name]
	_, schedOk := d.cfg.Schedules[name]
	_, stateOk := d.state.GetResource(name)
	if !svcOk && !cOk && !schedOk && !stateOk {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("resource %q not found", name)})
		return
	}

	// On-demand container logs: bypass ring buffer
	if source == "container" && (cOk || stateOk) {
		out, err := d.runtime.Logs(r.Context(), name, n)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(out)
		return
	}

	entries := d.logs.Lines(name, n, level)
	writeJSON(w, http.StatusOK, entries)
}

func (d *Daemon) handleStats(w http.ResponseWriter, r *http.Request) {
	type statsEntry struct {
		Name       string  `json:"name"`
		CPUPercent float64 `json:"cpu_percent"`
		MemUsageMB int64   `json:"mem_usage_mb"`
		MemLimitMB int64   `json:"mem_limit_mb"`
		PIDs       int     `json:"pids"`
	}

	var result []statsEntry
	for name := range d.cfg.Containers {
		rs, ok := d.state.GetResource(name)
		if !ok || rs.State != status.StateRunning {
			continue
		}
		if rs.Stats != nil {
			result = append(result, statsEntry{
				Name:       name,
				CPUPercent: rs.Stats.CPUPercent,
				MemUsageMB: rs.Stats.MemUsageMB,
				MemLimitMB: rs.Stats.MemLimitMB,
				PIDs:       rs.Stats.PIDs,
			})
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func (d *Daemon) handleStatsByName(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	_, inConfig := d.cfg.Containers[name]
	rs, inState := d.state.GetResource(name)
	if !inConfig && !inState {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("container %q not found", name)})
		return
	}
	if !inState {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("container %q not running", name)})
		return
	}
	if rs.Stats == nil {
		writeJSON(w, http.StatusOK, map[string]string{"status": "no stats available yet"})
		return
	}
	writeJSON(w, http.StatusOK, rs.Stats)
}

// resolveNames expands template names to their replica members.
// If a name matches a replica group template, all members are returned.
// Otherwise the name is returned as-is.
func (d *Daemon) resolveNames(names []string) []string {
	templateMap := make(map[string][]string)
	for _, g := range d.replicaGroups {
		templateMap[g.Template] = g.Names
	}

	var resolved []string
	for _, name := range names {
		if members, ok := templateMap[name]; ok {
			resolved = append(resolved, members...)
		} else {
			resolved = append(resolved, name)
		}
	}
	return resolved
}

func (d *Daemon) handleReplicas(w http.ResponseWriter, r *http.Request) {
	type replicaInfo struct {
		Template string                   `json:"template"`
		Count    int                      `json:"count"`
		Members  []map[string]interface{} `json:"members"`
	}

	var result []replicaInfo
	for _, g := range d.replicaGroups {
		info := replicaInfo{
			Template: g.Template,
			Count:    len(g.Names),
		}
		for _, name := range g.Names {
			member := map[string]interface{}{"name": name}
			if rs, ok := d.state.GetResource(name); ok {
				member["state"] = rs.State
				if rs.Stats != nil {
					member["cpu_percent"] = rs.Stats.CPUPercent
					member["mem_usage_mb"] = rs.Stats.MemUsageMB
				}
			} else {
				member["state"] = "unknown"
			}
			info.Members = append(info.Members, member)
		}
		result = append(result, info)
	}
	writeJSON(w, http.StatusOK, result)
}

func (d *Daemon) handleScale(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name     string `json:"name"`
		Replicas int    `json:"replicas"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	if body.Replicas < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "replicas must be >= 0"})
		return
	}

	// Find the replica group
	var group *config.ReplicaGroup
	for i := range d.replicaGroups {
		if d.replicaGroups[i].Template == body.Name {
			group = &d.replicaGroups[i]
			break
		}
	}
	if group == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("replica template %q not found", body.Name)})
		return
	}

	ctx := r.Context()
	current := len(group.Names)
	started := []string{}
	stopped := []string{}
	errors := map[string]string{}

	if body.Replicas > current {
		// Scale up: start new replicas
		// Find the base config from an existing member
		baseName := group.Names[0]
		baseCfg, ok := d.cfg.Containers[baseName]
		if !ok {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "base config not found"})
			return
		}

		for i := current + 1; i <= body.Replicas; i++ {
			newName := fmt.Sprintf("%s-%d", body.Name, i)

			// Create config with {name} substitution
			newCfg := baseCfg
			var vols []string
			for _, v := range baseCfg.Volumes {
				vols = append(vols, strings.ReplaceAll(
					strings.ReplaceAll(v, baseName, newName),
					baseName, newName,
				))
			}
			newCfg.Volumes = vols
			if baseCfg.Env != nil {
				newEnv := make(map[string]string, len(baseCfg.Env))
				for k, v := range baseCfg.Env {
					newEnv[k] = strings.ReplaceAll(v, baseName, newName)
				}
				newCfg.Env = newEnv
			}
			newCfg.Ports = nil // no port binds for new replicas

			d.cfg.Containers[newName] = newCfg
			rtCfg := containerToRuntimeConfig(newName, newCfg)
			if err := d.runtime.Run(ctx, newName, rtCfg); err != nil {
				errors[newName] = err.Error()
			} else {
				started = append(started, newName)
				d.state.SetResource(&status.ResourceStatus{
					Name:       newName,
					Type:       status.TypeContainer,
					State:      status.StateRunning,
					StartedAt:  time.Now(),
					ConfigHash: config.Hash(newCfg),
				})
				if newCfg.HealthCheck != nil {
					d.health.StartProbe(d.lifetimeCtx(), newName, newCfg.HealthCheck)
				}
			}
			group.Names = append(group.Names, newName)
		}
	} else if body.Replicas < current {
		// Scale down: stop excess replicas (from the end)
		for i := current; i > body.Replicas; i-- {
			removeName := group.Names[i-1]
			d.health.StopProbe(removeName)
			if err := d.runtime.Stop(ctx, removeName); err != nil {
				errors[removeName] = err.Error()
			} else {
				stopped = append(stopped, removeName)
				d.state.RemoveResource(removeName)
				delete(d.cfg.Containers, removeName)
			}
		}
		group.Names = group.Names[:body.Replicas]
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"template": body.Name,
		"previous": current,
		"current":  len(group.Names),
		"started":  started,
		"stopped":  stopped,
		"errors":   errors,
	})
}

func writeJSON(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(data)
}

// lifetimeCtx is the context for anything a request starts that must outlive
// it: service supervisors, health probes and schedules. A request's own
// r.Context() is cancelled when the handler returns, and exec.CommandContext
// kills the child when its context ends, so a service started under it died
// right after "skiff up/apply/restart" reported success (state "failed",
// LastError "context canceled"). Short calls that finish inside the handler
// (runtime Run/Stop/Exec) keep using the request context.
func (d *Daemon) lifetimeCtx() context.Context {
	if d.runCtx != nil {
		return d.runCtx
	}
	return context.Background()
}

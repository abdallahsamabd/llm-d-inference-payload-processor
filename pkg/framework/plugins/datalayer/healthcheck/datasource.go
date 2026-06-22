/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package healthcheck

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	dlsrc "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer/datasource"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
)

// PluginType is the identifier used when registering this datasource.
const PluginType = "health-check-datasource"

// compile-time interface assertion
var _ dlsrc.DataSource = &HealthCheckDataSource{}

const defaultReconcileInterval = 5 * time.Second

// DatasourcePluginConfig holds optional plugin-level defaults.
type DatasourcePluginConfig struct {
	ReconcileInterval string `json:"reconcileInterval,omitempty"`
}

// endpointState tracks consecutive successes/failures for one model.
type endpointState struct {
	consecutiveSuccesses int
	consecutiveFailures  int
	status               HealthStatus
	cancel               context.CancelFunc
}

// HealthCheckDataSource discovers health check configuration from the Datastore
// (written by model-config-datasource) and runs background probes accordingly.
// It reconciles periodically to pick up additions/removals at runtime.
type HealthCheckDataSource struct {
	typedName         plugin.TypedName
	ds                datalayer.Datastore
	reconcileInterval time.Duration
	states            map[string]*endpointState
	mu                sync.RWMutex
	stopCh            chan struct{}
	doneCh            chan struct{}
	wg                sync.WaitGroup
}

// DatasourceFactory creates a HealthCheckDataSource from plugin configuration.
func DatasourceFactory(name string, rawCfg json.RawMessage, h plugin.Handle) (plugin.Plugin, error) {
	reconcileInterval := defaultReconcileInterval

	if rawCfg != nil && len(rawCfg) > 0 {
		var cfg DatasourcePluginConfig
		if err := json.Unmarshal(rawCfg, &cfg); err == nil && cfg.ReconcileInterval != "" {
			if d, err := time.ParseDuration(cfg.ReconcileInterval); err == nil {
				reconcileInterval = d
			}
		}
	}

	return &HealthCheckDataSource{
		typedName:         plugin.TypedName{Type: PluginType, Name: name},
		ds:                h.Datastore(),
		reconcileInterval: reconcileInterval,
		states:            make(map[string]*endpointState),
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
	}, nil
}

func (d *HealthCheckDataSource) TypedName() plugin.TypedName { return d.typedName }

// Start launches a reconcile loop that periodically scans the Datastore for
// models with health check configuration and starts/stops probes dynamically.
func (d *HealthCheckDataSource) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("health-check-datasource")

	d.reconcile(ctx)

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		ticker := time.NewTicker(d.reconcileInterval)
		defer ticker.Stop()

		for {
			select {
			case <-d.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.reconcile(ctx)
			}
		}
	}()

	go func() {
		d.wg.Wait()
		close(d.doneCh)
	}()

	logger.Info("started health check datasource", "reconcileInterval", d.reconcileInterval)
	return nil
}

// Stop signals all goroutines to exit and blocks until they finish.
func (d *HealthCheckDataSource) Stop() {
	close(d.stopCh)

	d.mu.Lock()
	for _, state := range d.states {
		if state.cancel != nil {
			state.cancel()
		}
	}
	d.mu.Unlock()

	<-d.doneCh
}

// reconcile scans the Datastore for models with HealthCheckConfigKey attribute
// and starts/stops probe goroutines as needed.
func (d *HealthCheckDataSource) reconcile(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("health-check-datasource")

	models := d.ds.GetModels(func(m datalayer.Model) bool {
		_, ok := m.GetAttributes().Get(HealthCheckConfigKey)
		return ok
	})

	activeModels := make(map[string]struct{})

	for _, model := range models {
		name := model.GetName()
		activeModels[name] = struct{}{}

		raw, _ := model.GetAttributes().Get(HealthCheckConfigKey)
		cfg, ok := raw.(HealthCheckConfig)
		if !ok {
			continue
		}

		d.mu.RLock()
		_, exists := d.states[name]
		d.mu.RUnlock()

		if !exists {
			d.startCheck(ctx, name, cfg)
			logger.Info("started health check", "model", name, "url", cfg.URL)
		}
	}

	d.mu.Lock()
	for name, state := range d.states {
		if _, ok := activeModels[name]; !ok {
			if state.cancel != nil {
				state.cancel()
			}
			delete(d.states, name)
			logger.Info("stopped health check (model removed)", "model", name)
		}
	}
	d.mu.Unlock()
}

// startCheck launches a goroutine that probes the given model endpoint.
func (d *HealthCheckDataSource) startCheck(ctx context.Context, modelName string, cfg HealthCheckConfig) {
	interval := parseDurationOrDefault(cfg.Interval, 10*time.Second)
	timeout := parseDurationOrDefault(cfg.Timeout, 3*time.Second)
	unhealthyThreshold := cfg.UnhealthyThreshold
	if unhealthyThreshold == 0 {
		unhealthyThreshold = 3
	}
	healthyThreshold := cfg.HealthyThreshold
	if healthyThreshold == 0 {
		healthyThreshold = 2
	}

	checkType := cfg.Type
	if checkType == "" {
		checkType = "http"
	}

	checker := newChecker(CheckConfig{
		Model:   modelName,
		Type:    checkType,
		URL:     cfg.URL,
		Timeout: Duration{timeout},
	})

	checkCtx, cancel := context.WithCancel(ctx)

	d.mu.Lock()
	d.states[modelName] = &endpointState{status: Unknown, cancel: cancel}
	d.mu.Unlock()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.runCheck(checkCtx, modelName, checker, interval, timeout, unhealthyThreshold, healthyThreshold)
	}()
}

func (d *HealthCheckDataSource) runCheck(ctx context.Context, modelName string, checker Checker, interval, timeout time.Duration, unhealthyThreshold, healthyThreshold int) {
	logger := log.FromContext(ctx).WithName("health-check-datasource")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			probeCtx, cancel := context.WithTimeout(ctx, timeout)
			success := checker.Check(probeCtx)
			cancel()

			d.mu.RLock()
			state, exists := d.states[modelName]
			if !exists {
				d.mu.RUnlock()
				return
			}
			prevStatus := state.status
			d.mu.RUnlock()

			d.updateState(modelName, success, unhealthyThreshold, healthyThreshold)

			d.mu.RLock()
			newStatus := d.states[modelName].status
			d.mu.RUnlock()

			if prevStatus != newStatus {
				logger.Info("health status changed",
					"model", modelName,
					"from", string(prevStatus),
					"to", string(newStatus))
			}

			d.syncToDatastore(modelName)
		}
	}
}

func (d *HealthCheckDataSource) updateState(modelName string, probeSuccess bool, unhealthyThreshold, healthyThreshold int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	state, ok := d.states[modelName]
	if !ok {
		return
	}

	if probeSuccess {
		state.consecutiveSuccesses++
		state.consecutiveFailures = 0
		if state.consecutiveSuccesses >= healthyThreshold {
			state.status = Healthy
		}
	} else {
		state.consecutiveFailures++
		state.consecutiveSuccesses = 0
		if state.consecutiveFailures >= unhealthyThreshold {
			state.status = Unhealthy
		}
	}
}

func (d *HealthCheckDataSource) syncToDatastore(modelName string) {
	d.mu.RLock()
	state, ok := d.states[modelName]
	if !ok {
		d.mu.RUnlock()
		return
	}
	status := state.status
	d.mu.RUnlock()

	model := d.ds.GetOrCreateModel(modelName)
	model.GetAttributes().Put(HealthAttributeKey, status)
}

// GetStatus returns the current health status of a model (for testing).
func (d *HealthCheckDataSource) GetStatus(model string) HealthStatus {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if s, ok := d.states[model]; ok {
		return s.status
	}
	return Unknown
}

func parseDurationOrDefault(s string, def time.Duration) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

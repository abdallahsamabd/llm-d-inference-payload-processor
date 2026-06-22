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

package healthfilter

import (
	"context"
	"encoding/json"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/modelselector"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/datalayer/healthcheck"
)

const PluginType = "health-filter"

// compile-time interface assertion
var _ modelselector.Filter = &HealthFilterPlugin{}

// HealthFilterPlugin removes models marked as unhealthy from the candidate list.
// Models with no health attribute or "unknown" status are kept (safe default).
type HealthFilterPlugin struct {
	typedName plugin.TypedName
}

// FilterFactory creates a HealthFilterPlugin instance.
func FilterFactory(name string, _ json.RawMessage, _ plugin.Handle) (plugin.Plugin, error) {
	return NewHealthFilterPlugin().WithName(name), nil
}

// NewHealthFilterPlugin returns a new HealthFilterPlugin.
func NewHealthFilterPlugin() *HealthFilterPlugin {
	return &HealthFilterPlugin{
		typedName: plugin.TypedName{Type: PluginType, Name: PluginType},
	}
}

// TypedName returns the type and name tuple of this plugin instance.
func (p *HealthFilterPlugin) TypedName() plugin.TypedName { return p.typedName }

// WithName sets the instance name.
func (p *HealthFilterPlugin) WithName(name string) *HealthFilterPlugin {
	p.typedName.Name = name
	return p
}

// Filter removes unhealthy models from the candidate list.
// A model is excluded if its health-status attribute is "unhealthy".
// Models without a health status attribute (e.g., external models) or with "unknown" status pass through.
func (p *HealthFilterPlugin) Filter(ctx context.Context, _ *plugin.CycleState, _ *requesthandling.InferenceRequest, models []datalayer.Model) []datalayer.Model {
	logger := log.FromContext(ctx).WithName("health-filter")

	healthy := make([]datalayer.Model, 0, len(models))
	for _, model := range models {
		status := getHealthStatus(model)

		if status == healthcheck.Unhealthy {
			logger.V(1).Info("filtering out unhealthy model",
				"model", model.GetName(),
				"status", status)
			continue
		}
		healthy = append(healthy, model)
	}

	if len(healthy) == 0 && len(models) > 0 {
		logger.Info("all models are unhealthy, returning empty list")
	}

	return healthy
}

// getHealthStatus reads the health-status attribute from a model.
// Returns Unknown if the attribute is not set (external models, cold start).
func getHealthStatus(model datalayer.Model) healthcheck.HealthStatus {
	raw, ok := model.GetAttributes().Get(healthcheck.HealthAttributeKey)
	if !ok {
		return healthcheck.Unknown
	}
	status, ok := raw.(healthcheck.HealthStatus)
	if !ok {
		return healthcheck.Unknown
	}
	return status
}

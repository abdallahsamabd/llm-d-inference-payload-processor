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

import "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"

const (
	// HealthAttributeKey is the attribute key for the computed health status.
	HealthAttributeKey = "health-status"

	// HealthCheckConfigKey is the attribute key for per-model health check configuration.
	// Written by model-config-datasource or k8s-model-discovery-datasource,
	// read by health-check-datasource.
	HealthCheckConfigKey = "health-check-config"
)

// HealthStatus represents the health state of a model endpoint.
type HealthStatus string

const (
	Healthy   HealthStatus = "healthy"
	Unhealthy HealthStatus = "unhealthy"
	Unknown   HealthStatus = "unknown"
)

// Clone implements datalayer.Cloneable.
func (h HealthStatus) Clone() datalayer.Cloneable { return h }

// HealthCheckConfig is the per-model health check configuration stored in the Datastore.
// It is written by model-config-datasource (manual config) or k8s-model-discovery-datasource
// (auto-discovered from InferencePools), and consumed by health-check-datasource to probe.
type HealthCheckConfig struct {
	URL                string `json:"url"`
	Type               string `json:"type"`
	Interval           string `json:"interval"`
	Timeout            string `json:"timeout"`
	UnhealthyThreshold int    `json:"unhealthyThreshold"`
	HealthyThreshold   int    `json:"healthyThreshold"`
}

// Clone implements datalayer.Cloneable.
func (c HealthCheckConfig) Clone() datalayer.Cloneable {
	return c
}

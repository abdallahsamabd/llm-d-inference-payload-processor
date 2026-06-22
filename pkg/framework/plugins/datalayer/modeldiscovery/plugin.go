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

package modeldiscovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	inferenceapi "sigs.k8s.io/gateway-api-inference-extension/api/v1"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	dlsrc "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer/datasource"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/datalayer/healthcheck"
)

const PluginType = "k8s-model-discovery-datasource"

var _ dlsrc.DataSource = &ModelDiscoveryDataSource{}

// PluginConfig holds the plugin configuration.
type PluginConfig struct {
	DiscoveryInterval   string              `json:"discoveryInterval,omitempty"`
	Namespace           string              `json:"namespace,omitempty"`
	HealthCheckDefaults HealthCheckDefaults `json:"healthCheckDefaults,omitempty"`
}

// HealthCheckDefaults provides default health check parameters for discovered models.
type HealthCheckDefaults struct {
	Interval           string `json:"interval,omitempty"`
	Timeout            string `json:"timeout,omitempty"`
	UnhealthyThreshold int    `json:"unhealthyThreshold,omitempty"`
	HealthyThreshold   int    `json:"healthyThreshold,omitempty"`
}

// ModelDiscoveryDataSource watches InferencePools to discover model server pods
// and writes HealthCheckConfig to the Datastore for each discovered model.
// The health-check-datasource then picks up these configs and runs probes.
type ModelDiscoveryDataSource struct {
	typedName         plugin.TypedName
	k8sClient         client.Client
	ds                datalayer.Datastore
	namespace         string
	discoveryInterval time.Duration
	defaults          HealthCheckDefaults
	httpClient        *http.Client

	// discoveredModels tracks which models we discovered (model → pool name)
	discoveredModels map[string]string
	mu               sync.RWMutex
	stopCh           chan struct{}
	doneCh           chan struct{}
}

// DatasourceFactory creates a ModelDiscoveryDataSource from plugin configuration.
func DatasourceFactory(name string, rawCfg json.RawMessage, h plugin.Handle) (plugin.Plugin, error) {
	cfg := PluginConfig{
		DiscoveryInterval: "30s",
		HealthCheckDefaults: HealthCheckDefaults{
			Interval:           "10s",
			Timeout:            "3s",
			UnhealthyThreshold: 3,
			HealthyThreshold:   2,
		},
	}

	if rawCfg != nil && len(rawCfg) > 0 {
		if err := json.Unmarshal(rawCfg, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse config: %w", err)
		}
	}

	discoveryInterval, err := time.ParseDuration(cfg.DiscoveryInterval)
	if err != nil {
		discoveryInterval = 30 * time.Second
	}

	namespace := cfg.Namespace
	if namespace == "" {
		namespace = os.Getenv("NAMESPACE")
	}

	timeout, _ := time.ParseDuration(cfg.HealthCheckDefaults.Timeout)
	if timeout == 0 {
		timeout = 3 * time.Second
	}

	return &ModelDiscoveryDataSource{
		typedName:         plugin.TypedName{Type: PluginType, Name: name},
		k8sClient:         h.Client(),
		ds:                h.Datastore(),
		namespace:         namespace,
		discoveryInterval: discoveryInterval,
		defaults:          cfg.HealthCheckDefaults,
		httpClient:        &http.Client{Timeout: timeout},
		discoveredModels:  make(map[string]string),
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
	}, nil
}

func (d *ModelDiscoveryDataSource) TypedName() plugin.TypedName { return d.typedName }

// Start launches the periodic discovery loop.
func (d *ModelDiscoveryDataSource) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("model-discovery")

	d.discover(ctx)

	go func() {
		defer close(d.doneCh)
		ticker := time.NewTicker(d.discoveryInterval)
		defer ticker.Stop()

		for {
			select {
			case <-d.stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				d.discover(ctx)
			}
		}
	}()

	logger.Info("started model discovery", "namespace", d.namespace, "interval", d.discoveryInterval)
	return nil
}

// Stop signals the discovery loop to exit and blocks until it finishes.
func (d *ModelDiscoveryDataSource) Stop() {
	close(d.stopCh)
	<-d.doneCh
}

// discover lists InferencePools, finds matching pods, probes /v1/models,
// and writes HealthCheckConfig to the Datastore for each discovered model.
func (d *ModelDiscoveryDataSource) discover(ctx context.Context) {
	logger := log.FromContext(ctx).WithName("model-discovery")

	pools, err := d.listInferencePools(ctx)
	if err != nil {
		logger.Error(err, "failed to list InferencePools")
		return
	}

	currentModels := make(map[string]string) // model → pool

	for i := range pools {
		pool := &pools[i]
		poolName := pool.Name
		selector := pool.Spec.Selector.MatchLabels
		if len(selector) == 0 || len(pool.Spec.TargetPorts) == 0 {
			continue
		}

		port := int(pool.Spec.TargetPorts[0].Number)

		pods, err := d.listReadyPods(ctx, selector)
		if err != nil {
			logger.Error(err, "failed to list pods for pool", "pool", poolName)
			continue
		}

		if len(pods) == 0 {
			logger.V(1).Info("no ready pods for pool", "pool", poolName)
			continue
		}

		// Probe pods to discover models and build the endpoint list
		podEndpoints := make([]string, 0, len(pods))
		allModels := make(map[string]struct{})

		for j := range pods {
			pod := &pods[j]
			endpoint := fmt.Sprintf("http://%s:%d/v1/models", pod.Status.PodIP, port)
			podEndpoints = append(podEndpoints, endpoint)

			models, err := d.probeModels(ctx, endpoint)
			if err != nil {
				logger.V(1).Info("probe failed", "pod", pod.Name, "endpoint", endpoint, "error", err)
				continue
			}
			for _, m := range models {
				allModels[m] = struct{}{}
			}
		}

		if len(podEndpoints) == 0 {
			continue
		}

		// Write HealthCheckConfig for each discovered model using the first ready pod endpoint.
		// The health-check-datasource will probe this endpoint to validate model presence.
		for modelName := range allModels {
			currentModels[modelName] = poolName
			d.writeHealthCheckConfig(modelName, podEndpoints[0])
		}

		logger.V(1).Info("discovered models from pool",
			"pool", poolName,
			"modelCount", len(allModels),
			"podCount", len(pods))
	}

	// Prune models that are no longer discovered from any pool
	d.mu.Lock()
	for model, oldPool := range d.discoveredModels {
		if _, ok := currentModels[model]; !ok {
			logger.Info("model no longer discovered, cleaning up", "model", model, "pool", oldPool)
			m := d.ds.GetOrCreateModel(model)
			m.GetAttributes().Delete(healthcheck.HealthCheckConfigKey)
		}
	}
	d.discoveredModels = currentModels
	d.mu.Unlock()
}

// writeHealthCheckConfig writes a HealthCheckConfig attribute to the Datastore.
func (d *ModelDiscoveryDataSource) writeHealthCheckConfig(modelName, url string) {
	model := d.ds.GetOrCreateModel(modelName)
	cfg := healthcheck.HealthCheckConfig{
		URL:                url,
		Type:               "http",
		Interval:           d.defaults.Interval,
		Timeout:            d.defaults.Timeout,
		UnhealthyThreshold: d.defaults.UnhealthyThreshold,
		HealthyThreshold:   d.defaults.HealthyThreshold,
	}
	model.GetAttributes().Put(healthcheck.HealthCheckConfigKey, cfg)
}

// listInferencePools returns all InferencePools in the configured namespace.
func (d *ModelDiscoveryDataSource) listInferencePools(ctx context.Context) ([]inferenceapi.InferencePool, error) {
	list := &inferenceapi.InferencePoolList{}
	opts := &client.ListOptions{}
	if d.namespace != "" {
		opts.Namespace = d.namespace
	}
	if err := d.k8sClient.List(ctx, list, opts); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// listReadyPods returns pods matching the selector that are Ready and have an IP.
func (d *ModelDiscoveryDataSource) listReadyPods(ctx context.Context, selector map[inferenceapi.LabelKey]inferenceapi.LabelValue) ([]corev1.Pod, error) {
	// Convert GAIE LabelSelector types to standard string map
	labelMap := make(map[string]string, len(selector))
	for k, v := range selector {
		labelMap[string(k)] = string(v)
	}

	podList := &corev1.PodList{}
	opts := &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(labelMap),
	}
	if d.namespace != "" {
		opts.Namespace = d.namespace
	}

	if err := d.k8sClient.List(ctx, podList, opts); err != nil {
		return nil, err
	}

	ready := make([]corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		if isPodReady(&podList.Items[i]) {
			ready = append(ready, podList.Items[i])
		}
	}
	return ready, nil
}

// modelsResponse represents the OpenAI-compatible /v1/models response.
type modelsResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

// probeModels sends GET /v1/models to the endpoint and returns all model IDs.
func (d *ModelDiscoveryDataSource) probeModels(ctx context.Context, url string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var models modelsResponse
	if err := json.Unmarshal(body, &models); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(models.Data))
	for _, m := range models.Data {
		if m.ID != "" {
			names = append(names, m.ID)
		}
	}
	return names, nil
}

// isPodReady returns true if the pod has the Ready condition and a PodIP assigned.
func isPodReady(pod *corev1.Pod) bool {
	if pod.Status.PodIP == "" {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

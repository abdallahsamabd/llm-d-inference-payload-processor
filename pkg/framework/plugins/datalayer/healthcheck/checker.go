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
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// Checker performs a single health check probe.
type Checker interface {
	Check(ctx context.Context) bool
}

// modelsResponse represents the OpenAI-compatible /v1/models response.
type modelsResponse struct {
	Data []struct {
		ID string `json:"id"`
	} `json:"data"`
}

// ModelHTTPChecker sends GET /v1/models and verifies that the expected model
// name exists in the returned list. This covers TCP connectivity (connection
// failure = unhealthy), server liveness (non-2xx = unhealthy), and model
// availability (model name missing from response = unhealthy).
type ModelHTTPChecker struct {
	URL       string
	ModelName string
	Client    *http.Client
}

func (c *ModelHTTPChecker) Check(ctx context.Context) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL, nil)
	if err != nil {
		return false
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false
	}

	var models modelsResponse
	if err := json.Unmarshal(body, &models); err != nil {
		return false
	}

	for _, m := range models.Data {
		if m.ID == c.ModelName {
			return true
		}
	}
	return false
}

// CheckConfig is an internal struct used by newChecker to construct a Checker.
type CheckConfig struct {
	Model   string
	Type    string
	URL     string
	Timeout Duration
}

// Duration wraps time.Duration for internal use.
type Duration struct {
	time.Duration
}

// newChecker creates a Checker for the given check configuration.
func newChecker(cfg CheckConfig) Checker {
	timeout := cfg.Timeout.Duration
	if timeout == 0 {
		timeout = 3 * time.Second
	}

	var client *http.Client
	if cfg.Type == "https" {
		client = &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
			},
		}
	} else {
		client = &http.Client{Timeout: timeout}
	}

	return &ModelHTTPChecker{URL: cfg.URL, ModelName: cfg.Model, Client: client}
}

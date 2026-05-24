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

package modelselector

import (
	"context"
	"testing"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/datastore"
	fwkplugin "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
)

func newTestDatastore(modelNames ...string) datastore.Datastore {
	ds := datastore.NewFakeDataStore()
	for _, name := range modelNames {
		ds.GetOrCreateModel(name)
	}
	return ds
}

func TestProcessRequestWritesSelectedModelToBodyAndCycleState(t *testing.T) {
	ds := newTestDatastore("model-a", "model-b", "model-c")
	plugin, err := NewModelSelectorPlugin(ds)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	request := requesthandling.NewInferenceRequest()
	request.Body["model"] = "auto"
	cycleState := fwkplugin.NewCycleState()

	err = plugin.ProcessRequest(context.Background(), cycleState, request)
	if err != nil {
		t.Fatalf("ProcessRequest failed: %v", err)
	}

	selectedModel, ok := request.Body["model"].(string)
	if !ok || selectedModel == "" {
		t.Fatal("expected model field in request body to be set")
	}
	if selectedModel == "auto" {
		t.Error("expected model field to be replaced with selected model, still 'auto'")
	}

	storedModel, err := fwkplugin.ReadCycleStateKey[string](cycleState, SelectedModelKey)
	if err != nil {
		t.Fatalf("expected selected model in CycleState: %v", err)
	}
	if storedModel != selectedModel {
		t.Errorf("CycleState model %q does not match body model %q", storedModel, selectedModel)
	}
}

func TestProcessRequestSelectsFromDatastoreModels(t *testing.T) {
	candidates := []string{"llama-70b", "llama-8b", "mistral-7b"}
	ds := newTestDatastore(candidates...)
	plugin, err := NewModelSelectorPlugin(ds)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	request := requesthandling.NewInferenceRequest()
	request.Body["model"] = "auto"
	cycleState := fwkplugin.NewCycleState()

	err = plugin.ProcessRequest(context.Background(), cycleState, request)
	if err != nil {
		t.Fatalf("ProcessRequest failed: %v", err)
	}

	selectedModel := request.Body["model"].(string)
	found := false
	for _, c := range candidates {
		if c == selectedModel {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("selected model %q is not in datastore models %v", selectedModel, candidates)
	}
}

func TestProcessRequestFailsWithEmptyDatastore(t *testing.T) {
	ds := newTestDatastore() // no models
	plugin, err := NewModelSelectorPlugin(ds)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	request := requesthandling.NewInferenceRequest()
	request.Body["model"] = "auto"
	cycleState := fwkplugin.NewCycleState()

	err = plugin.ProcessRequest(context.Background(), cycleState, request)
	if err == nil {
		t.Fatal("expected error with empty datastore")
	}
}

func TestNewModelSelectorPluginRejectsNilDatastore(t *testing.T) {
	_, err := NewModelSelectorPlugin(nil)
	if err == nil {
		t.Fatal("expected error for nil datastore")
	}
}

func TestTypedName(t *testing.T) {
	ds := newTestDatastore("model-a")
	plugin, _ := NewModelSelectorPlugin(ds)
	if plugin.TypedName().Type != ModelSelectorPluginType {
		t.Errorf("expected type %q, got %q", ModelSelectorPluginType, plugin.TypedName().Type)
	}
}

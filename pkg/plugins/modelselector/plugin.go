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
	"encoding/json"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/datastore"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/modelselector"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
	ms "github.com/llm-d/llm-d-inference-payload-processor/pkg/modelselector"
)

const (
	ModelSelectorPluginType = "model-selector"

	// CycleState key where the selected model name is stored for downstream plugins.
	SelectedModelKey = "selected-model"
)

var _ requesthandling.RequestProcessor = &ModelSelectorPlugin{}

// ModelSelectorPluginFactory is the factory function for the ModelSelector RequestProcessor plugin.
func ModelSelectorPluginFactory(name string, _ json.RawMessage, handle plugin.Handle) (plugin.Plugin, error) {
	// TODO: once PR #84 merges, get Datastore from handle via handle.Datastore()
	// For now, Datastore is passed directly to the constructor.
	_ = handle

	return nil, fmt.Errorf("'%s' plugin must be created via NewModelSelectorPlugin, not via factory (Datastore not yet available in Handle)", ModelSelectorPluginType)
}

// NewModelSelectorPlugin creates a ModelSelector RequestProcessor plugin.
// Candidate models are read from the Datastore on each request.
func NewModelSelectorPlugin(ds datastore.Datastore) (*ModelSelectorPlugin, error) {
	if ds == nil {
		return nil, fmt.Errorf("datastore is required for '%s' plugin", ModelSelectorPluginType)
	}

	// Build profile with picker only.
	// Filters and scorers can be added via WithFilters() / WithScorers()
	// once implementations are available.
	profile := ms.NewModelSelectorProfile().
		WithPicker(&defaultPicker{})

	selector := ms.NewModelSelector(profile)

	return &ModelSelectorPlugin{
		typedName: plugin.TypedName{Type: ModelSelectorPluginType, Name: ModelSelectorPluginType},
		selector:  selector,
		datastore: ds,
	}, nil
}

// ModelSelectorPlugin is a RequestProcessor that runs the ModelSelector
// pipeline (Filter → Score → Pick) to select a model for the request.
// Candidate models are read from the Datastore on each request.
type ModelSelectorPlugin struct {
	typedName plugin.TypedName
	selector  *ms.ModelSelector
	datastore datastore.Datastore
}

func (p *ModelSelectorPlugin) TypedName() plugin.TypedName {
	return p.typedName
}

// ProcessRequest reads candidate models from the Datastore, runs model
// selection, and writes the selected model into the request body and CycleState.
func (p *ModelSelectorPlugin) ProcessRequest(ctx context.Context, cycleState *plugin.CycleState, request *requesthandling.InferenceRequest) error {
	logger := log.FromContext(ctx)

	candidateModels := p.loadCandidateModels()
	if len(candidateModels) == 0 {
		return errors.New("no candidate models available in datastore")
	}

	result, err := p.selector.Select(ctx, request, cycleState, candidateModels)
	if err != nil {
		return fmt.Errorf("model selection failed: %w", err)
	}

	selectedName := result.TargetModel.GetName()
	logger.V(logutil.VERBOSE).Info("Model selected", "model", selectedName)

	cycleState.Write(SelectedModelKey, selectedName)
	request.SetBodyField("model", selectedName)

	return nil
}

// loadCandidateModels reads all known models from the Datastore.
func (p *ModelSelectorPlugin) loadCandidateModels() []datalayer.Model {
	modelNames := p.datastore.Models()
	candidates := make([]datalayer.Model, len(modelNames))
	for i, name := range modelNames {
		candidates[i] = p.datastore.GetOrCreateModel(name)
	}
	return candidates
}

// defaultPicker picks the model with the highest score.
// Replace with MaxScorePicker or WeightedRandomPicker once PR #74
// pickers are wired with factory functions.
type defaultPicker struct{}

func (p *defaultPicker) TypedName() plugin.TypedName {
	return plugin.TypedName{Type: "default-picker", Name: "default-picker"}
}

func (p *defaultPicker) Pick(_ context.Context, _ *plugin.CycleState, scoredModels []*modelselector.ScoredModel) *modelselector.ProfileRunResult {
	if len(scoredModels) == 0 {
		return nil
	}
	best := scoredModels[0]
	for _, sm := range scoredModels[1:] {
		if sm.Score > best.Score {
			best = sm
		}
	}
	return &modelselector.ProfileRunResult{TargetModel: best.Model}
}

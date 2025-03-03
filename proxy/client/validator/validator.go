// Copyright 2018-2023 contributors to the OpenLineage project
// SPDX-License-Identifier: Apache-2.0

package validator

import (
	"encoding/json"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/santhosh-tekuri/jsonschema/v5/httploader"
	"io"
	"path"
)

var schemaURLs = map[string]string{
	"ErrorMessageRunFacet.json":                "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
	"ExternalQueryRunFacet.json":               "https://openlineage.io/spec/facets/1-0-0/ExternalQueryRunFacet.json",
	"ExtractionErrorRunFacet.json":             "https://openlineage.io/spec/facets/1-0-0/ExtractionErrorRunFacet.json",
	"NominalTimeRunFacet.json":                 "https://openlineage.io/spec/facets/1-0-0/NominalTimeRunFacet.json",
	"ParentRunFacet.json":                      "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
	"ProcessingEngineRunFacet.json":            "https://openlineage.io/spec/facets/1-0-0/ProcessingEngineRunFacet.json",
	"DocumentationJobFacet.json":               "https://openlineage.io/spec/facets/1-0-0/DocumentationJobFacet.json",
	"OwnershipJobFacet.json":                   "https://openlineage.io/spec/facets/1-0-0/OwnershipJobFacet.json",
	"SourceCodeJobFacet.json":                  "https://openlineage.io/spec/facets/1-0-0/SourceCodeJobFacet.json",
	"SourceCodeLocationJobFacet.json":          "https://openlineage.io/spec/facets/1-0-0/SourceCodeLocationJobFacet.json",
	"SQLJobFacet.json":                         "https://openlineage.io/spec/facets/1-0-0/SQLJobFacet.json",
	"ColumnLineageDatasetFacet.json":           "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json",
	"DatasetVersionDatasetFacet.json":          "https://openlineage.io/spec/facets/1-0-0/DatasetVersionDatasetFacet.json",
	"DatasourceDatasetFacet.json":              "https://openlineage.io/spec/facets/1-0-0/DatasourceDatasetFacet.json",
	"DataQualityAssertionsDatasetFacet.json":   "https://openlineage.io/spec/facets/1-0-0/DataQualityAssertionsDatasetFacet.json",
	"DocumentationDatasetFacet.json":           "https://openlineage.io/spec/facets/1-0-0/DocumentationDatasetFacet.json",
	"LifecycleStateChangeDatasetFacet.json":    "https://openlineage.io/spec/facets/1-0-0/LifecycleStateChangeDatasetFacet.json",
	"OwnershipDatasetFacet.json":               "https://openlineage.io/spec/facets/1-0-0/OwnershipDatasetFacet.json",
	"SchemaDatasetFacet.json":                  "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json",
	"StorageDatasetFacet.json":                 "https://openlineage.io/spec/facets/1-0-0/StorageDatasetFacet.json",
	"SymlinksDatasetFacet.json":                "https://openlineage.io/spec/facets/1-0-0/SymlinksDatasetFacet.json",
	"DataQualityMetricsInputDatasetFacet.json": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsInputDatasetFacet.json",
	"OutputStatisticsOutputDatasetFacet.json":  "https://openlineage.io/spec/facets/1-0-0/OutputStatisticsOutputDatasetFacet.json",
}

type validator struct {
	mainSchema             *jsonschema.Schema
	baseRunFacetSchema     *jsonschema.Schema
	runFacetSchemas        []*jsonschema.Schema
	baseJobFacetSchema     *jsonschema.Schema
	jobFacetSchemas        []*jsonschema.Schema
	baseInputFacetSchema   *jsonschema.Schema
	inputFacetSchemas      []*jsonschema.Schema
	baseOutputFacetSchema  *jsonschema.Schema
	outputFacetSchemas     []*jsonschema.Schema
	baseDatasetFacetSchema *jsonschema.Schema
	datasetFacetSchemas    []*jsonschema.Schema
}

// IEventValidator is an interface for the event validator.
type IEventValidator interface {
	Validate(event string) error
}

// Validate is the function that implements interface of IEventValidator, and it is responsible for
// validating if the given event is compliant with the schema defined in OpenLineage spec.
func (v *validator) Validate(event string) error {
	var doc interface{}
	if err := json.Unmarshal([]byte(event), &doc); err != nil {
		return err
	}

	if err := v.mainSchema.Validate(doc); err != nil {
		return err
	}
	runEvent := doc.(map[string]interface{})

	run := runEvent["run"].(map[string]interface{})
	if facets, ok := run["facets"]; ok {
		for _, facetSchema := range v.runFacetSchemas {
			if err := facetSchema.Validate(facets); err != nil {
				return err
			}
		}
		for _, facet := range facets.(map[string]interface{}) {
			if err := v.baseRunFacetSchema.Validate(facet); err != nil {
				return err
			}
		}
	}

	job := runEvent["job"].(map[string]interface{})
	if facets, ok := job["facets"]; ok {
		for _, facetSchema := range v.jobFacetSchemas {
			if err := facetSchema.Validate(facets); err != nil {
				return err
			}
		}
		for _, facet := range facets.(map[string]interface{}) {
			if err := v.baseJobFacetSchema.Validate(facet); err != nil {
				return err
			}
		}
	}

	if inputs, ok := runEvent["inputs"]; ok {
		for _, input := range inputs.([]interface{}) {
			if facets, ok := input.(map[string]interface{})["inputFacets"]; ok {
				for _, facetSchema := range v.inputFacetSchemas {
					if err := facetSchema.Validate(facets); err != nil {
						return err
					}
				}
				for _, facet := range facets.(map[string]interface{}) {
					if err := v.baseInputFacetSchema.Validate(facet); err != nil {
						return err
					}
				}
			}
			if facets, ok := input.(map[string]interface{})["facets"]; ok {
				for _, facetSchema := range v.datasetFacetSchemas {
					if err := facetSchema.Validate(facets); err != nil {
						return err
					}
				}
				for _, facet := range facets.(map[string]interface{}) {
					if err := v.baseDatasetFacetSchema.Validate(facet); err != nil {
						return err
					}
				}
			}
		}
	}

	if outputs, ok := runEvent["outputs"]; ok {
		for _, output := range outputs.([]interface{}) {
			if facets, ok := output.(map[string]interface{})["outputFacets"]; ok {
				for _, facetSchema := range v.outputFacetSchemas {
					if err := facetSchema.Validate(facets); err != nil {
						return err
					}
				}
				for _, facet := range facets.(map[string]interface{}) {
					if err := v.baseOutputFacetSchema.Validate(facet); err != nil {
						return err
					}
				}
			}
			if facets, ok := output.(map[string]interface{})["facets"]; ok {
				for _, facetSchema := range v.datasetFacetSchemas {
					if err := facetSchema.Validate(facets); err != nil {
						return err
					}
				}
				for _, facet := range facets.(map[string]interface{}) {
					if err := v.baseDatasetFacetSchema.Validate(facet); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func New() (*validator, error) {
	jsonschema.Loaders["https"] = loadSchema

	compiler := jsonschema.NewCompiler()
	compiler.AssertFormat = true

	mainSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json")
	if err != nil {
		return nil, err
	}

	baseRunFacetSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/RunFacet")
	if err != nil {
		return nil, err
	}

	runFacetSchemas, err := compileRunFacetSchemas(compiler)
	if err != nil {
		return nil, err
	}

	baseJobFacetSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/JobFacet")
	if err != nil {
		return nil, err
	}

	jobFacetSchemas, err := compileJobFacetSchemas(compiler)
	if err != nil {
		return nil, err
	}

	baseInputFacetSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/InputDatasetFacet")
	if err != nil {
		return nil, err
	}

	inputFacetSchemas, err := compileInputFacetSchemas(compiler)
	if err != nil {
		return nil, err
	}

	baseOutputFacetSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/OutputDatasetFacet")
	if err != nil {
		return nil, err
	}

	outputFacetSchemas, err := compileOutputFacetSchemas(compiler)
	if err != nil {
		return nil, err
	}

	baseDatasetFacetSchema, err := compiler.Compile("https://openlineage.io/spec/1-0-5/OpenLineage.json#/$defs/DatasetFacet")
	if err != nil {
		return nil, err
	}

	datasetFacetSchemas, err := compileDatasetFacetSchemas(compiler)
	if err != nil {
		return nil, err
	}

	return &validator{
		mainSchema:             mainSchema,
		baseRunFacetSchema:     baseRunFacetSchema,
		baseJobFacetSchema:     baseJobFacetSchema,
		runFacetSchemas:        runFacetSchemas,
		jobFacetSchemas:        jobFacetSchemas,
		baseInputFacetSchema:   baseInputFacetSchema,
		inputFacetSchemas:      inputFacetSchemas,
		baseOutputFacetSchema:  baseOutputFacetSchema,
		outputFacetSchemas:     outputFacetSchemas,
		baseDatasetFacetSchema: baseDatasetFacetSchema,
		datasetFacetSchemas:    datasetFacetSchemas,
	}, nil
}

func compileRunFacetSchemas(compiler *jsonschema.Compiler) ([]*jsonschema.Schema, error) {
	facets := []string{
		"ErrorMessageRunFacet.json",
		"ExternalQueryRunFacet.json",
		"ExtractionErrorRunFacet.json",
		"NominalTimeRunFacet.json",
		"ParentRunFacet.json",
		"ProcessingEngineRunFacet.json",
	}
	return compileFacetSchemas(compiler, facets)
}

func compileJobFacetSchemas(compiler *jsonschema.Compiler) ([]*jsonschema.Schema, error) {
	facets := []string{
		"DocumentationJobFacet.json",
		"OwnershipJobFacet.json",
		"SourceCodeJobFacet.json",
		"SourceCodeLocationJobFacet.json",
		"SQLJobFacet.json",
	}
	return compileFacetSchemas(compiler, facets)
}

func compileInputFacetSchemas(compiler *jsonschema.Compiler) ([]*jsonschema.Schema, error) {
	facets := []string{"DataQualityMetricsInputDatasetFacet.json"}
	return compileFacetSchemas(compiler, facets)
}

func compileOutputFacetSchemas(compiler *jsonschema.Compiler) ([]*jsonschema.Schema, error) {
	facets := []string{"OutputStatisticsOutputDatasetFacet.json"}
	return compileFacetSchemas(compiler, facets)
}

func compileDatasetFacetSchemas(compiler *jsonschema.Compiler) ([]*jsonschema.Schema, error) {
	facets := []string{
		"ColumnLineageDatasetFacet.json",
		"DatasetVersionDatasetFacet.json",
		"DatasourceDatasetFacet.json",
		"DataQualityAssertionsDatasetFacet.json",
		"DocumentationDatasetFacet.json",
		"LifecycleStateChangeDatasetFacet.json",
		"OwnershipDatasetFacet.json",
		"SchemaDatasetFacet.json",
		"StorageDatasetFacet.json",
		"SymlinksDatasetFacet.json",
	}
	return compileFacetSchemas(compiler, facets)
}

func compileFacetSchemas(compiler *jsonschema.Compiler, facets []string) ([]*jsonschema.Schema, error) {
	schemas := make([]*jsonschema.Schema, len(facets))
	for i, schemaKey := range facets {
		schemaURL := schemaURLs[schemaKey]
		schema, err := compiler.Compile(schemaURL)
		if err != nil {
			return nil, err
		}
		schemas[i] = schema
	}
	return schemas, nil
}

func loadSchema(url string) (io.ReadCloser, error) {
	if schemaURL, ok := schemaURLs[path.Base(url)]; ok {
		return httploader.Load(schemaURL)
	} else {
		return httploader.Load(url)
	}
}

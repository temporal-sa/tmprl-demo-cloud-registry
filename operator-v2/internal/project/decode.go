package project

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"
)

const (
	MaxDocumentBytes       = 1024 * 1024
	MaxExpandedScalarBytes = 4 * MaxDocumentBytes
)

//go:embed schemas/project.schema.json
var schemaBytes []byte
var schemaOnce sync.Once
var compiledSchema *jsonschema.Schema
var schemaError error

type Issue struct {
	Pointer string `json:"pointer"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Line    int    `json:"line,omitempty"`
	Column  int    `json:"column,omitempty"`
}
type ValidationError struct {
	Issues []Issue `json:"issues"`
}

func (e *ValidationError) Error() string {
	if len(e.Issues) == 0 {
		return "invalid project"
	}
	i := e.Issues[0]
	return fmt.Sprintf("%s: %s (%s)", i.Pointer, i.Message, i.Code)
}
func issue(pointer, code, message string) error {
	return &ValidationError{Issues: []Issue{{Pointer: pointer, Code: code, Message: message}}}
}

type noRemoteSchemas struct{}

func (noRemoteSchemas) Load(string) (any, error) {
	return nil, errors.New("external schema loading is disabled")
}

func schema() (*jsonschema.Schema, error) {
	schemaOnce.Do(func() {
		var value any
		if schemaError = json.Unmarshal(schemaBytes, &value); schemaError != nil {
			return
		}
		compiler := jsonschema.NewCompiler()
		compiler.UseLoader(noRemoteSchemas{})
		compiler.DefaultDraft(jsonschema.Draft2020)
		const location = "https://github.com/temporal-sa/tmprl-demo-cloud-demo-registry/schemas/project.schema.json"
		if schemaError = compiler.AddResource(location, value); schemaError != nil {
			return
		}
		compiledSchema, schemaError = compiler.Compile(location)
	})
	return compiledSchema, schemaError
}

// Decode accepts exactly one JSON/YAML authoring document. Errors include field
// locations and safe explanations, never the rejected scalar values.
func Decode(data []byte) (Document, error) {
	if len(data) > MaxDocumentBytes {
		return Document{}, issue("", "DocumentTooLarge", "project exceeds the document size limit")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var node yaml.Node
	if err := decoder.Decode(&node); err != nil {
		return Document{}, issue("", "InvalidYAML", "cannot parse project YAML or JSON")
	}
	var second yaml.Node
	if err := decoder.Decode(&second); err != io.EOF {
		return Document{}, issue("", "MultipleDocuments", "exactly one complete document is required")
	}
	if len(node.Content) != 1 || node.Content[0].Kind != yaml.MappingNode {
		return Document{}, issue("", "InvalidDocument", "project must be an object")
	}
	budget := decodeBudget{}
	if err := checkNode(node.Content[0], "", 0, &budget); err != nil {
		return Document{}, err
	}
	var value map[string]any
	if err := node.Decode(&value); err != nil {
		return Document{}, issue("", "InvalidDocument", "document contains unsupported values")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return Document{}, issue("", "InvalidDocument", "document must contain JSON-compatible values")
	}
	if len(encoded) > MaxDocumentBytes {
		return Document{}, issue("", "DocumentTooLarge", "expanded JSON representation exceeds the document size limit")
	}
	// Normalize numeric representation for the schema validator.
	var generic any
	if err = json.Unmarshal(encoded, &generic); err != nil {
		return Document{}, err
	}
	validator, err := schema()
	if err != nil {
		return Document{}, fmt.Errorf("compile embedded project schema: %w", err)
	}
	if err = validator.Validate(generic); err != nil {
		return Document{}, safeSchemaIssues(err)
	}
	var document Document
	if err = json.Unmarshal(encoded, &document); err != nil {
		return Document{}, issue("", "UnsupportedShape", "validated project cannot be represented by the typed contract")
	}
	if issues := ValidateSemantics(document); len(issues) > 0 {
		return Document{}, &ValidationError{Issues: issues}
	}
	return document, nil
}

type decodeBudget struct {
	nodes       int
	scalarBytes int
}

func checkNode(node *yaml.Node, pointer string, depth int, budget *decodeBudget) error {
	budget.nodes++
	if depth > 128 || budget.nodes > 100000 {
		return issue(pointer, "DocumentComplexity", "project exceeds nesting or node limits")
	}
	if node.Kind == yaml.AliasNode {
		if node.Alias == nil {
			return issue(pointer, "InvalidYAMLAlias", "alias has no defined target")
		}
		// Follow aliases under the same depth/node budget before the decoder
		// expands them. Cycles and expansion bombs fail without leaking values.
		return checkNode(node.Alias, pointer, depth+1, budget)
	}
	if node.Kind == yaml.ScalarNode {
		budget.scalarBytes += len(node.Value)
		if budget.scalarBytes > MaxExpandedScalarBytes {
			return issue(pointer, "DocumentComplexity", "expanded scalar data exceeds the document budget")
		}
		switch node.Tag {
		case "!!str", "!!int", "!!float", "!!bool", "!!null":
		default:
			return issue(pointer, "UnsupportedYAMLTag", "scalar must have a JSON-compatible YAML type")
		}
	}
	if node.Kind == yaml.MappingNode {
		seen := map[string]bool{}
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			budget.scalarBytes += len(key.Value)
			if budget.scalarBytes > MaxExpandedScalarBytes {
				return issue(pointer, "DocumentComplexity", "expanded key data exceeds the document budget")
			}
			if key.Tag == "!!merge" && key.Value == "<<" {
				if err := checkNode(node.Content[i+1], pointer, depth+1, budget); err != nil {
					return err
				}
				continue
			}
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
				return issue(pointer, "InvalidMappingKey", "object keys must be strings")
			}
			path := pointer + "/" + escapePointer(key.Value)
			if seen[key.Value] {
				return &ValidationError{Issues: []Issue{{Pointer: path, Code: "DuplicateKey", Message: "object key is repeated", Line: key.Line, Column: key.Column}}}
			}
			seen[key.Value] = true
			if err := checkNode(node.Content[i+1], path, depth+1, budget); err != nil {
				return err
			}
		}
	} else {
		for i, child := range node.Content {
			if err := checkNode(child, fmt.Sprintf("%s/%d", pointer, i), depth+1, budget); err != nil {
				return err
			}
		}
	}
	return nil
}

func escapePointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}

func safeSchemaIssues(err error) error {
	var validation *jsonschema.ValidationError
	if !errors.As(err, &validation) {
		return issue("", "SchemaViolation", "project does not satisfy its schema")
	}
	var issues []Issue
	var collect func(*jsonschema.ValidationError)
	collect = func(e *jsonschema.ValidationError) {
		if len(issues) >= 32 {
			return
		}
		if len(e.Causes) > 0 {
			for _, cause := range e.Causes {
				collect(cause)
			}
			return
		}
		path := ""
		for _, part := range e.InstanceLocation {
			path += "/" + escapePointer(part)
		}
		keyword := "schema"
		if parts := e.ErrorKind.KeywordPath(); len(parts) > 0 {
			keyword = parts[len(parts)-1]
		}
		issues = append(issues, Issue{Pointer: path, Code: "SchemaViolation", Message: "field does not satisfy " + keyword + " constraint"})
	}
	collect(validation)
	sort.Slice(issues, func(i, j int) bool {
		if issues[i].Pointer == issues[j].Pointer {
			return issues[i].Message < issues[j].Message
		}
		return issues[i].Pointer < issues[j].Pointer
	})
	return &ValidationError{Issues: issues}
}

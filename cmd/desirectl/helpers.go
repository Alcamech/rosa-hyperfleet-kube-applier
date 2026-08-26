package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	kubeapplier "github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
	sigsyaml "sigs.k8s.io/yaml"
)

const taskKey = "desirectl"

func resourceRefFromManifest(raw []byte) (kubeapplier.ResourceReference, error) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return kubeapplier.ResourceReference{}, fmt.Errorf("manifest must be JSON: %w", err)
	}

	apiVersion, _ := obj["apiVersion"].(string)
	kind, _ := obj["kind"].(string)
	metadata, _ := obj["metadata"].(map[string]any)

	if apiVersion == "" || kind == "" {
		return kubeapplier.ResourceReference{}, fmt.Errorf("manifest must have apiVersion and kind")
	}

	info, err := resourceTypeFromManifest(apiVersion, kind)
	if err != nil {
		return kubeapplier.ResourceReference{}, err
	}

	ref := kubeapplier.ResourceReference{
		Group:    info.Group,
		Version:  info.Version,
		Resource: info.Resource,
	}

	if metadata != nil {
		if name, ok := metadata["name"].(string); ok {
			ref.Name = name
		}
		if ns, ok := metadata["namespace"].(string); ok {
			ref.Namespace = ns
		}
	}

	if ref.Name == "" {
		return ref, fmt.Errorf("manifest must have metadata.name")
	}

	return ref, nil
}

func splitYAMLDocuments(data []byte) [][]byte {
	var docs [][]byte
	for _, doc := range bytes.Split(data, []byte("\n---")) {
		trimmed := bytes.TrimSpace(doc)
		if len(trimmed) == 0 {
			continue
		}
		docs = append(docs, trimmed)
	}
	return docs
}

func readManifestFiles(paths []string) ([][]byte, error) {
	var allDocs [][]byte

	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, fmt.Errorf("accessing %s: %w", p, err)
		}

		if info.IsDir() {
			err := filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.IsDir() {
					return nil
				}
				ext := strings.ToLower(filepath.Ext(path))
				if ext != ".yaml" && ext != ".yml" && ext != ".json" {
					return nil
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return fmt.Errorf("reading %s: %w", path, err)
				}
				allDocs = append(allDocs, splitYAMLDocuments(data)...)
				return nil
			})
			if err != nil {
				return nil, err
			}
		} else {
			data, err := os.ReadFile(p)
			if err != nil {
				return nil, fmt.Errorf("reading %s: %w", p, err)
			}
			allDocs = append(allDocs, splitYAMLDocuments(data)...)
		}
	}

	return allDocs, nil
}

func toJSON(data []byte) ([]byte, error) {
	return sigsyaml.YAMLToJSON(data)
}

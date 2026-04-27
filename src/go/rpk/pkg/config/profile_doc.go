// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// profileFieldDocs contains documentation for profile fields, keyed by their
// YAML path. These are displayed as comments when editing a profile.
var profileFieldDocs = map[string]string{
	"name":        "Unique identifier for this profile",
	"description": "Human-readable description of this profile",
	"prompt":      "Custom shell prompt format; overrides globals.prompt if set",

	"kafka_api":                          "Kafka API connection configuration",
	"kafka_api.brokers":                  "Comma-separated list of broker addresses (host:port)",
	"kafka_api.tls":                      "TLS configuration for Kafka API connections",
	"kafka_api.tls.key_file":             "Path to client private key file for mTLS",
	"kafka_api.tls.cert_file":            "Path to client certificate file for mTLS",
	"kafka_api.tls.ca_file":              "Path to CA certificate file for TLS verification",
	"kafka_api.tls.insecure_skip_verify": "Skip TLS certificate verification (not recommended for production)",
	"kafka_api.sasl":                     "SASL authentication configuration",
	"kafka_api.sasl.user":                "username for authentication",
	"kafka_api.sasl.password":            "password for authentication",
	"kafka_api.sasl.mechanism":           "SCRAM-SHA-256, SCRAM-SHA-512, PLAIN, or OAUTHBEARER",

	"admin_api":                          "Admin API connection configuration",
	"admin_api.addresses":                "Comma-separated list of Admin API addresses (host:port)",
	"admin_api.tls":                      "TLS configuration for Admin API connections",
	"admin_api.tls.key_file":             "Path to client private key file for mTLS",
	"admin_api.tls.cert_file":            "Path to client certificate file for mTLS",
	"admin_api.tls.ca_file":              "Path to CA certificate file for TLS verification",
	"admin_api.tls.insecure_skip_verify": "Skip TLS certificate verification (not recommended for production)",

	"schema_registry":                          "Schema Registry connection configuration",
	"schema_registry.addresses":                "Comma-separated list of Schema Registry addresses (host:port)",
	"schema_registry.tls":                      "TLS configuration for Schema Registry connections",
	"schema_registry.tls.key_file":             "Path to client private key file for mTLS",
	"schema_registry.tls.cert_file":            "Path to client certificate file for mTLS",
	"schema_registry.tls.ca_file":              "Path to CA certificate file for TLS verification",
	"schema_registry.tls.insecure_skip_verify": "Skip TLS certificate verification (not recommended for production)",
}

// excludedFields contains fields that should not be shown in the documented
// output because they are internal or managed by rpk.
var excludedFields = map[string]struct{}{
	"cloud_environment": {},
	"license_check":     {},
}

// noCommentFields contains fields that should be shown but without any
// documentation comments (e.g., rpk-cloud-managed fields). If a parent field
// is listed, all nested children are also treated as no-comment fields.
var noCommentFields = map[string]struct{}{
	"from_cloud":    {},
	"cloud_cluster": {},
}

// isNoCommentField returns true if the given path or any of its parent paths
// are in noCommentFields.
func isNoCommentField(path string) bool {
	for path != "" {
		if _, ok := noCommentFields[path]; ok {
			return true
		}
		idx := strings.LastIndex(path, ".")
		if idx == -1 {
			return false
		}
		path = path[:idx]
	}
	return false
}

// ProfileToDocumentedYAML generates a YAML representation of the profile with
// all available fields shown. Fields that have values are displayed normally,
// while unset fields are shown as comments with documentation.
func ProfileToDocumentedYAML(p RpkProfile) ([]byte, error) {
	var buf bytes.Buffer

	buf.WriteString("# Available fields are shown below. Uncomment and edit as needed.\n")
	buf.WriteString("# For more information, run: rpk profile set --help\n\n")

	if err := writeDocumentedStruct(&buf, reflect.ValueOf(p), reflect.TypeFor[RpkProfile](), "", 0); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeDocumentedStruct writes a struct to the buffer with documentation.
func writeDocumentedStruct(buf *bytes.Buffer, v reflect.Value, t reflect.Type, pathPrefix string, indent int) error {
	indentStr := strings.Repeat("  ", indent)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i)

		// Unexported/Internal fields should be skipped.
		if !field.IsExported() {
			continue
		}

		// Parse yaml tag: 'yaml:"name,omitempty"' -> 'name'
		yamlTag := field.Tag.Get("yaml")
		if yamlTag == "" || yamlTag == "-" {
			continue
		}
		tagParts := strings.Split(yamlTag, ",")
		fieldName := tagParts[0]

		// Build full path for doc lookup.
		fullPath := fieldName
		if pathPrefix != "" {
			fullPath = pathPrefix + "." + fieldName
		}

		if _, ok := excludedFields[fullPath]; ok {
			continue
		}

		noComment := isNoCommentField(fullPath)
		isEmpty := isEmptyValue(fieldValue)

		// noComment fields that are empty are skipped entirely.
		if noComment && isEmpty {
			continue
		}

		var doc string
		if !noComment {
			doc = profileFieldDocs[fullPath]
		}

		if isEmpty {
			writeCommentedField(buf, fieldName, field.Type, doc, fullPath, indentStr)
		} else {
			// Hack: Add warning before cloud-managed fields.
			if fullPath == "from_cloud" {
				fmt.Fprintf(buf, "%s# The following fields are managed by rpk. Altering them manually may\n", indentStr)
				fmt.Fprintf(buf, "%s# result in unexpected behavior.\n", indentStr)
			}
			if err := writeValueField(buf, fieldName, fieldValue, doc, fullPath, indentStr); err != nil {
				return err
			}
		}

		// Add blank line after top-level fields for readability (except before cloud_cluster)
		if indent == 0 && fullPath != "from_cloud" {
			buf.WriteString("\n")
		}
	}
	return nil
}

// isStructAllEmpty checks if a struct has all empty/zero fields.
func isStructAllEmpty(v reflect.Value) bool {
	// I checked for struct before calling this function. However, since
	// it's reflection-based, let's be safe.
	if v.Kind() != reflect.Struct {
		return false
	}
	for i := 0; i < v.NumField(); i++ {
		if v.Type().Field(i).IsExported() && !isEmptyValue(v.Field(i)) {
			return false
		}
	}
	return true
}

// isEmptyValue checks if a reflect.Value is considered "empty".
// Note: A non-nil pointer to an empty struct is NOT considered empty,
// as it may be semantically meaningful (e.g., tls: {} enables TLS).
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Pointer:
		// A non-nil pointer is not empty, even if it points to a zero struct.
		// This preserves semantics like `tls: {}` which signals TLS is enabled.
		return v.IsNil()
	case reflect.Interface:
		return v.IsNil()
	case reflect.Slice, reflect.Map:
		return v.IsNil() || v.Len() == 0
	case reflect.String:
		return v.String() == ""
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() && !isEmptyValue(v.Field(i)) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// writeCommentedField writes a field as a YAML comment.
func writeCommentedField(buf *bytes.Buffer, name string, t reflect.Type, doc, fullPath, indent string) {
	// Write documentation comment
	if doc != "" {
		fmt.Fprintf(buf, "%s# %s\n", indent, doc)
	}

	// Handle pointer types
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	// Write commented field based on type
	switch t.Kind() {
	case reflect.Struct:
		// Write struct template as comment
		fmt.Fprintf(buf, "%s# %s:\n", indent, name)
		writeCommentedStructTemplate(buf, t, fullPath, indent+"  ")
	case reflect.Slice:
		fmt.Fprintf(buf, "%s# %s: []\n", indent, name)
	case reflect.Bool:
		fmt.Fprintf(buf, "%s# %s: false\n", indent, name)
	case reflect.String:
		fmt.Fprintf(buf, "%s# %s: \"\"\n", indent, name)
	default:
		fmt.Fprintf(buf, "%s# %s:\n", indent, name)
	}
}

// writeCommentedStructTemplate writes a struct template as comments.
func writeCommentedStructTemplate(buf *bytes.Buffer, t reflect.Type, pathPrefix, indent string) {
	for field := range t.Fields() {
		field := field
		if !field.IsExported() {
			continue
		}

		yamlTag := field.Tag.Get("yaml")
		if yamlTag == "" || yamlTag == "-" {
			continue
		}

		tagParts := strings.Split(yamlTag, ",")
		fieldName := tagParts[0]
		if fieldName == "" {
			fieldName = strings.ToLower(field.Name)
		}

		fullPath := pathPrefix + "." + fieldName

		// Skip excluded
		if _, ok := excludedFields[fieldName]; ok {
			continue
		}

		// Skip noCommentFields entirely in commented templates
		if isNoCommentField(fullPath) {
			continue
		}

		doc := profileFieldDocs[fullPath]
		if doc != "" {
			fmt.Fprintf(buf, "%s# %s\n", indent, doc)
		}

		fieldType := field.Type
		if fieldType.Kind() == reflect.Pointer {
			fieldType = fieldType.Elem()
		}

		switch fieldType.Kind() {
		case reflect.Struct:
			fmt.Fprintf(buf, "%s# %s:\n", indent, fieldName)
			writeCommentedStructTemplate(buf, fieldType, fullPath, indent+"  ")
		case reflect.Slice:
			fmt.Fprintf(buf, "%s# %s: []\n", indent, fieldName)
		case reflect.Bool:
			fmt.Fprintf(buf, "%s# %s: false\n", indent, fieldName)
		case reflect.String:
			fmt.Fprintf(buf, "%s# %s: \"\"\n", indent, fieldName)
		default:
			fmt.Fprintf(buf, "%s# %s:\n", indent, fieldName)
		}
	}
}

// writeValueField writes a field with its actual value.
func writeValueField(buf *bytes.Buffer, name string, v reflect.Value, doc, fullPath, indent string) error {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			writeCommentedField(buf, name, v.Type(), doc, fullPath, indent)
			return nil
		}
		v = v.Elem()
	}

	// Write documentation comment on top (if present)
	if doc != "" {
		fmt.Fprintf(buf, "%s# %s\n", indent, doc)
	}

	switch v.Kind() {
	case reflect.Struct:
		// If struct is empty, write as {} to preserve semantics. (e.g., tls: {})
		if isStructAllEmpty(v) {
			fmt.Fprintf(buf, "%s%s: {}\n", indent, name)
		} else {
			fmt.Fprintf(buf, "%s%s:\n", indent, name)
			if err := writeDocumentedStruct(buf, v, v.Type(), fullPath, len(indent)/2+1); err != nil {
				return err
			}
		}

	case reflect.Slice:
		fmt.Fprintf(buf, "%s%s:\n", indent, name)
		for i := 0; i < v.Len(); i++ {
			elem := v.Index(i)
			if err := writeSliceElement(buf, elem, indent+"  "); err != nil {
				return err
			}
		}

	case reflect.String:
		val := v.String()
		if needsQuoting(val) {
			val = fmt.Sprintf("%q", val)
		}
		fmt.Fprintf(buf, "%s%s: %s\n", indent, name, val)

	case reflect.Bool:
		val := "false"
		if v.Bool() {
			val = "true"
		}
		fmt.Fprintf(buf, "%s%s: %s\n", indent, name, val)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fmt.Fprintf(buf, "%s%s: %d\n", indent, name, v.Int())

	default:
		// Use yaml.Marshal for other types.
		valBytes, err := yaml.Marshal(v.Interface())
		if err != nil {
			return fmt.Errorf("marshal field %q: %w", name, err)
		}
		val := strings.TrimSpace(string(valBytes))
		fmt.Fprintf(buf, "%s%s: %s\n", indent, name, val)
	}
	return nil
}

func writeSliceElement(buf *bytes.Buffer, v reflect.Value, indent string) error {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.String:
		val := v.String()
		if needsQuoting(val) {
			val = fmt.Sprintf("%q", val)
		}
		fmt.Fprintf(buf, "%s- %s\n", indent, val)

	case reflect.Struct:
		// First field on same line as dash, rest indented
		t := v.Type()
		first := true
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if !field.IsExported() {
				continue
			}
			yamlTag := field.Tag.Get("yaml")
			if yamlTag == "" || yamlTag == "-" {
				continue
			}
			tagParts := strings.Split(yamlTag, ",")
			fieldName := tagParts[0]

			fieldValue := v.Field(i)
			if isEmptyValue(fieldValue) {
				continue
			}

			if first {
				fmt.Fprintf(buf, "%s- %s: ", indent, fieldName)
				if err := writeInlineValue(buf, fieldValue); err != nil {
					return err
				}
				buf.WriteString("\n")
				first = false
			} else {
				fmt.Fprintf(buf, "%s  %s: ", indent, fieldName)
				if err := writeInlineValue(buf, fieldValue); err != nil {
					return err
				}
				buf.WriteString("\n")
			}
		}
		if first {
			// Empty struct
			fmt.Fprintf(buf, "%s- {}\n", indent)
		}

	default:
		valBytes, err := yaml.Marshal(v.Interface())
		if err != nil {
			return fmt.Errorf("marshal slice element: %w", err)
		}
		fmt.Fprintf(buf, "%s- %s", indent, strings.TrimSpace(string(valBytes)))
		if !strings.HasSuffix(string(valBytes), "\n") {
			buf.WriteString("\n")
		}
	}
	return nil
}

func writeInlineValue(buf *bytes.Buffer, v reflect.Value) error {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			buf.WriteString("null")
			return nil
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.String:
		val := v.String()
		if needsQuoting(val) {
			fmt.Fprintf(buf, "%q", val)
		} else {
			buf.WriteString(val)
		}
	case reflect.Bool:
		if v.Bool() {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fmt.Fprintf(buf, "%d", v.Int())
	default:
		valBytes, err := yaml.Marshal(v.Interface())
		if err != nil {
			return fmt.Errorf("marshal value: %w", err)
		}
		buf.WriteString(strings.TrimSpace(string(valBytes)))
	}
	return nil
}

// needsQuoting returns true if a string needs to be quoted in YAML. It checks
// for special characters, leading/trailing whitespace, and values that may be
// interpreted as bool/null (YAML 1.1/1.2 compatibility).
func needsQuoting(s string) bool {
	if s == "" {
		return true
	}
	// Characters with special meaning in YAML (indicators, anchors, etc.)
	if strings.ContainsAny(s, ":{}[]!@#$%^&*|'\"`\\") {
		return true
	}
	// leading/trailing whitespace
	if strings.TrimSpace(s) != s {
		return true
	}
	// Values that yaml.v3 may interpret as bool/null when decoding into typed
	// fields. See https://pkg.go.dev/gopkg.in/yaml.v3 for YAML 1.1/1.2
	// compatibility details.
	lower := strings.ToLower(s)
	switch lower {
	case "true", "false", "null", "yes", "no", "on", "off":
		return true
	}
	return false
}

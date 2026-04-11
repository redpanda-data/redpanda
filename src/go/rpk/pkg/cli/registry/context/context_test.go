// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package context_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	srcontext "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry/context"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry"
)

var testSchema = sr.Schema{
	Schema: `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`,
	Type:   sr.TypeAvro,
}

func setupRegistryTest(t *testing.T, reg *srfake.Registry) *cobra.Command {
	t.Helper()
	fs := afero.NewMemMapFs()
	cfgBody := fmt.Sprintf(`current_profile: test
profiles:
    - name: test
      schema_registry:
        addresses:
            - %s
`, reg.URL())
	require.NoError(t, afero.WriteFile(fs, "/tmp/rpk.yaml", []byte(cfgBody), 0o644))

	p := &config.Params{
		ConfigFlag: "/tmp/rpk.yaml",
		Formatter:  config.OutFormatter{Kind: "text"},
	}
	return registry.NewCommand(fs, p)
}

func setupRegistryTestWithAdmin(t *testing.T, reg *srfake.Registry, adminURL string) *cobra.Command {
	t.Helper()
	fs := afero.NewMemMapFs()
	cfgBody := fmt.Sprintf(`current_profile: test
profiles:
    - name: test
      schema_registry:
        addresses:
            - %s
      admin_api:
        addresses:
            - %s
`, reg.URL(), adminURL)
	require.NoError(t, afero.WriteFile(fs, "/tmp/rpk.yaml", []byte(cfgBody), 0o644))

	p := &config.Params{
		ConfigFlag: "/tmp/rpk.yaml",
		Formatter:  config.OutFormatter{Kind: "text"},
	}
	return registry.NewCommand(fs, p)
}

// fakeAdminAPI returns an httptest.Server that responds to the
// cluster_config endpoint with the given config value.
func fakeAdminAPI(t *testing.T, configValue map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/cluster_config" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(configValue)
			return
		}
		// Return empty JSON for other endpoints (e.g., license checks).
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{})
	}))
}

// captureOutput captures stdout during function execution. This is needed
// because the context commands use fmt.Println/fmt.Printf to write output.
func captureOutput(f func()) string {
	r, w, _ := os.Pipe()
	oldStdout := os.Stdout
	os.Stdout = w
	f()
	w.Close()
	os.Stdout = oldStdout
	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

func TestContextList(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema("plain-topic", 1, 1, testSchema)
	reg.SeedSchema(":.ctx1:my-subject", 1, 2, testSchema)
	reg.SeedSchema(":.ctx2:other-subject", 1, 3, testSchema)

	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"context", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	// Table output should include header and context names.
	require.Contains(t, output, "CONTEXT")
	require.Contains(t, output, "MODE")
	require.Contains(t, output, "COMPATIBILITY")
	require.Contains(t, output, ".ctx1")
	require.Contains(t, output, ".ctx2")
}

func TestContextListEmpty(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"context", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	// Should show table with at least the default context.
	require.Contains(t, output, "CONTEXT")
}

func TestContextListWithSchemaContext(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema(":.ctx1:my-subject", 1, 1, testSchema)
	reg.SeedSchema(":.ctx2:other-subject", 1, 2, testSchema)

	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"--schema-context", ".ctx1", "--skip-context-check", "context", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	// GET /contexts returns all contexts regardless of --schema-context.
	require.Contains(t, output, "CONTEXT")
	require.Contains(t, output, ".ctx1")
	require.Contains(t, output, ".ctx2")
}

func TestContextDelete(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	// The rpk command first lists contexts to verify the target exists,
	// then calls DELETE. Use an interceptor so GET /contexts includes
	// .ctx1, while the registry has no subjects in that context (so the
	// DELETE succeeds with 204).
	reg.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "GET" && r.URL.Path == "/contexts" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode([]string{".", ".ctx1"})
			return true
		}
		return false
	})

	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"context", "delete", ".ctx1", "--no-confirm"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	require.Contains(t, output, `Successfully deleted context ".ctx1".`)
}

func TestListContextsUnsupported(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	// Simulate an older Redpanda that returns 404 for GET /contexts.
	reg.Intercept(func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/contexts" {
			http.Error(w, "Not found", http.StatusNotFound)
			return true
		}
		return false
	})

	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	require.NoError(t, err)

	_, err = srcontext.ListContexts(context.Background(), cl)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema registry contexts are not supported by this cluster")
}

func TestSubjectListWithSchemaContext(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema("plain-topic", 1, 1, testSchema)
	reg.SeedSchema(":.ctx1:my-subject", 1, 2, testSchema)
	reg.SeedSchema(":.ctx1:another", 1, 3, testSchema)
	reg.SeedSchema(":.ctx2:other-subject", 1, 4, testSchema)

	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"--schema-context", ".ctx1", "--skip-context-check", "subject", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	// Should show only subjects in .ctx1, with qualifier stripped.
	require.Contains(t, output, "another")
	require.Contains(t, output, "my-subject")
	require.NotContains(t, output, "plain-topic")
	require.NotContains(t, output, "other-subject")
	// Should NOT contain the context qualifier in the output.
	require.NotContains(t, output, ":.ctx1:")
}

func TestSubjectDeleteWithSchemaContext(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema(":.ctx1:my-subject", 1, 1, testSchema)

	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"--schema-context", ".ctx1", "--skip-context-check", "subject", "delete", "my-subject"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	// The display should show the unqualified subject name.
	require.Contains(t, output, "my-subject")
	require.NotContains(t, output, ":.ctx1:")

	// Verify the qualified subject was actually deleted.
	require.False(t, reg.SubjectExists(":.ctx1:my-subject"))
}

func TestSchemaListWithSchemaContext(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema("plain-topic", 1, 1, testSchema)
	reg.SeedSchema(":.ctx1:my-subject", 1, 2, testSchema)
	reg.SeedSchema(":.ctx2:other-subject", 1, 3, testSchema)

	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"--schema-context", ".ctx1", "--skip-context-check", "schema", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	// Should show only schemas in .ctx1, with qualifier stripped.
	require.Contains(t, output, "my-subject")
	require.NotContains(t, output, "plain-topic")
	require.NotContains(t, output, "other-subject")
	require.NotContains(t, output, ":.ctx1:")
}

func TestAdminAPICheckFlagEnabled(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema(":.ctx1:my-subject", 1, 1, testSchema)

	admin := fakeAdminAPI(t, map[string]any{
		"schema_registry_enable_qualified_subjects": true,
	})
	defer admin.Close()

	cmd := setupRegistryTestWithAdmin(t, reg, admin.URL)
	cmd.SetArgs([]string{"--schema-context", ".ctx1", "subject", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	require.Contains(t, output, "my-subject")
	require.NotContains(t, output, ":.ctx1:")
}

func TestAdminAPICheckFlagDisabled(t *testing.T) {
	admin := fakeAdminAPI(t, map[string]any{
		"schema_registry_enable_qualified_subjects": false,
	})
	defer admin.Close()

	fs := afero.NewMemMapFs()
	cfgBody := fmt.Sprintf(`current_profile: test
profiles:
    - name: test
      admin_api:
        addresses:
            - %s
`, admin.URL)
	require.NoError(t, afero.WriteFile(fs, "/tmp/rpk.yaml", []byte(cfgBody), 0o644))
	p := &config.Params{ConfigFlag: "/tmp/rpk.yaml"}
	profile, err := p.LoadVirtualProfile(fs)
	require.NoError(t, err)

	err = srcontext.IsContextSupported(context.Background(), fs, profile, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema contexts are not enabled on this cluster")
}

func TestAdminAPICheckSkipped(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema(":.ctx1:my-subject", 1, 1, testSchema)

	// No admin API server — would fail without --skip-context-check.
	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"--schema-context", ".ctx1", "--skip-context-check", "subject", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	require.Contains(t, output, "my-subject")
}

func TestSubjectListContextColumn(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema("plain-topic", 1, 1, testSchema)
	reg.SeedSchema(":.ctx1:my-subject", 1, 2, testSchema)

	// Contexts supported + admin check skipped → CONTEXT column should appear.
	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"--skip-context-check", "subject", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	require.Contains(t, output, "CONTEXT")
	require.Contains(t, output, "SUBJECT")
	require.Contains(t, output, "default")
	require.Contains(t, output, ".ctx1")
	require.Contains(t, output, "plain-topic")
	require.Contains(t, output, "my-subject")
	// Raw qualified prefix should not appear.
	require.NotContains(t, output, ":.ctx1:")
}

func TestSubjectListNoContextColumn(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema("plain-topic", 1, 1, testSchema)
	reg.SeedSchema("another-topic", 1, 2, testSchema)

	// Simulate a cluster that does not support contexts: admin API
	// reports qualified_subjects as false.
	admin := fakeAdminAPI(t, map[string]any{
		"schema_registry_enable_qualified_subjects": false,
	})
	defer admin.Close()

	cmd := setupRegistryTestWithAdmin(t, reg, admin.URL)
	cmd.SetArgs([]string{"subject", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	require.NotContains(t, output, "CONTEXT")
	require.Contains(t, output, "another-topic")
	require.Contains(t, output, "plain-topic")
}

func TestSubjectListContextColumnHiddenWithSchemaContext(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema("plain-topic", 1, 1, testSchema)
	reg.SeedSchema(":.ctx1:my-subject", 1, 2, testSchema)

	// --schema-context filters results → no CONTEXT column.
	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"--schema-context", ".ctx1", "--skip-context-check", "subject", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	require.NotContains(t, output, "CONTEXT")
	require.Contains(t, output, "my-subject")
}

func TestSchemaListContextColumn(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema("plain-topic", 1, 1, testSchema)
	reg.SeedSchema(":.ctx1:my-subject", 1, 2, testSchema)

	// Contexts supported + admin check skipped → CONTEXT column should appear.
	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"--skip-context-check", "schema", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	require.Contains(t, output, "CONTEXT")
	require.Contains(t, output, "SUBJECT")
	require.Contains(t, output, "default")
	require.Contains(t, output, ".ctx1")
	require.Contains(t, output, "plain-topic")
	require.Contains(t, output, "my-subject")
	require.NotContains(t, output, ":.ctx1:")
}

func TestSchemaListNoContextColumn(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema("plain-topic", 1, 1, testSchema)

	// Simulate a cluster that does not support contexts: admin API
	// reports qualified_subjects as false.
	admin := fakeAdminAPI(t, map[string]any{
		"schema_registry_enable_qualified_subjects": false,
	})
	defer admin.Close()

	cmd := setupRegistryTestWithAdmin(t, reg, admin.URL)
	cmd.SetArgs([]string{"schema", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	require.NotContains(t, output, "CONTEXT")
	require.Contains(t, output, "plain-topic")
}

func TestSubjectListContextColumnWithAdminAPI(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema("plain-topic", 1, 1, testSchema)
	reg.SeedSchema(":.ctx1:my-subject", 1, 2, testSchema)

	admin := fakeAdminAPI(t, map[string]any{
		"schema_registry_enable_qualified_subjects": true,
	})
	defer admin.Close()

	// No --skip-context-check: the probe verifies both /contexts and admin API.
	cmd := setupRegistryTestWithAdmin(t, reg, admin.URL)
	cmd.SetArgs([]string{"subject", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	require.Contains(t, output, "CONTEXT")
	require.Contains(t, output, "default")
	require.Contains(t, output, ".ctx1")
	require.Contains(t, output, "plain-topic")
	require.Contains(t, output, "my-subject")
}

func TestSchemaListContextColumnHiddenWithSchemaContext(t *testing.T) {
	reg := srfake.New()
	defer reg.Close()

	reg.SeedSchema("plain-topic", 1, 1, testSchema)
	reg.SeedSchema(":.ctx1:my-subject", 1, 2, testSchema)

	// --schema-context filters results → no CONTEXT column.
	cmd := setupRegistryTest(t, reg)
	cmd.SetArgs([]string{"--schema-context", ".ctx1", "--skip-context-check", "schema", "list"})

	output := captureOutput(func() {
		require.NoError(t, cmd.Execute())
	})

	require.NotContains(t, output, "CONTEXT")
	require.Contains(t, output, "my-subject")
}

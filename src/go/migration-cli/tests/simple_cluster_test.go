package tests

import (
	"fmt"
	"os"
	"testing"

	"github.com/r3labs/diff/v3"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"

	"github.com/redpanda-data/redpanda/src/go/migration-cli/cmd"
)

var (
	dataDir            = "data"
	filesToTest        = []string{"simple_cluster", "simple_resources_listeners", "additional_configs", "complex_cluster_001"}
	consoleFilesToTest = []string{"00-console", "01-console-admin-api", "02-console-kafka-mtls"}
)

func Test_Migration(t *testing.T) {
	cleanup := make([]string, 0)
	for i := range filesToTest {
		t.Run(fmt.Sprintf("file %s", filesToTest[i]), func(t *testing.T) {
			file := filesToTest[i]
			flags := &cmd.RootFlags{
				ClusterFile:               fmt.Sprintf("%s/%s.yaml", dataDir, file),
				OutputFile:                fmt.Sprintf("%s/%s_test_output.yaml", dataDir, file),
				ChartVersion:              cmd.DefaultChartRefVersion,
				RedpandaImageTag:          "v23.1.10",
				RedpandaContainerRegistry: "docker.redpanda.com/redpandadata/redpanda",
			}

			err := cmd.DoMigration(flags)
			assert.NoError(t, err, "should not have error in doing migration")

			actualData, err := os.ReadFile(flags.OutputFile)
			assert.NoError(t, err, "could not read output data")

			actual := &unstructured.Unstructured{}
			err = yaml.Unmarshal(actualData, actual)
			assert.NoError(t, err, "could not unmarshal output data")

			expectedOuputName := fmt.Sprintf("%s/%s_output.yaml", dataDir, file)
			expectedData, err := os.ReadFile(expectedOuputName)
			assert.NoError(t, err, "could not read expected data")

			expected := &unstructured.Unstructured{}
			err = yaml.Unmarshal(expectedData, expected)
			assert.NoError(t, err, "could not unmarshal expected data")

			cleanup = append(cleanup, flags.OutputFile)

			changelog, err := diff.Diff(expected, actual)
			assert.NoError(t, err, "could not check diff between expected and actual")

			if len(changelog) > 0 {
				for j := range changelog {
					t.Logf("%v", changelog[j])
				}
			}

			assert.Len(t, changelog, 0, fmt.Sprintf("%q has a non zero changelog", file))
		})
	}

	doCleanup(cleanup)
}

func Test_Console_Migration(t *testing.T) {
	cleanup := make([]string, 0)
	for i := range consoleFilesToTest {
		t.Run(fmt.Sprintf("file %s", consoleFilesToTest[i]), func(t *testing.T) {
			file := consoleFilesToTest[i]
			flags := &cmd.RootFlags{
				ClusterFile:               fmt.Sprintf("%s/%s-rp.yaml", dataDir, file),
				ConsoleFile:               fmt.Sprintf("%s/%s.yaml", dataDir, file),
				OutputFile:                fmt.Sprintf("%s/%s-rp_test_output.yaml", dataDir, file),
				ChartVersion:              cmd.DefaultChartRefVersion,
				RedpandaImageTag:          "v23.1.10",
				RedpandaContainerRegistry: "docker.redpanda.com/redpandadata/redpanda",
			}

			err := cmd.DoMigration(flags)
			assert.NoError(t, err, "should not have error in doing migration")

			actualData, err := os.ReadFile(flags.OutputFile)
			assert.NoError(t, err, "could not read output data")

			actual := &unstructured.Unstructured{}
			err = yaml.Unmarshal(actualData, actual)
			assert.NoError(t, err, "could not unmarshal output data")

			expectedOuputName := fmt.Sprintf("%s/%s-rp_output.yaml", dataDir, file)
			expectedData, err := os.ReadFile(expectedOuputName)
			assert.NoError(t, err, "could not read expected data")

			expected := &unstructured.Unstructured{}
			err = yaml.Unmarshal(expectedData, expected)
			assert.NoError(t, err, "could not unmarshal expected data")

			cleanup = append(cleanup, flags.OutputFile)

			changelog, err := diff.Diff(expected, actual)
			assert.NoError(t, err, "could not check diff between expected and actual")

			if len(changelog) > 0 {
				for j := range changelog {
					t.Logf("%v", changelog[j])
				}
			}

			assert.Len(t, changelog, 0, fmt.Sprintf("%q has a non zero changelog", file))
		})
	}

	doCleanup(cleanup)
}

func doCleanup(cleanFiles []string) {
	for i := range cleanFiles {
		cf := cleanFiles[i]
		err := os.Remove(cf)
		if err != nil {
			fmt.Printf("could not delete %s: %s\n", cf, err)
		}
	}
}

package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"vectorized/pkg/cli/ui"
	sandbox "vectorized/pkg/redpanda/sandbox"
	"vectorized/pkg/redpanda/sandbox/docker"

	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"
)

type sbParams struct {
	sbDir string
	fs    afero.Fs
}

func NewSandboxCommand(fs afero.Fs) *cobra.Command {
	sbParams := sbParams{
		fs: fs,
	}
	command := &cobra.Command{
		Use:   "sandbox <action>",
		Short: "Manage redpanda sandbox",
	}
	command.PersistentFlags().StringVar(&sbParams.sbDir, "sandbox-dir",
		filepath.Join(os.Getenv("HOME"), "redpanda/sandbox"),
		"Path to the directory where Redpanda sandbox state will be stored,"+
			" defaults to ${HOME}/redpanda/sandbox")
	command.AddCommand(newCreateCommand(&sbParams))
	command.AddCommand(newStartCommand(&sbParams))
	command.AddCommand(newStopCommand(&sbParams))
	command.AddCommand(newRestartCommand(&sbParams))
	command.AddCommand(newWipeCommand(&sbParams))
	command.AddCommand(newDestroyCommand(&sbParams))
	command.AddCommand(newStatusCommand(&sbParams))
	command.AddCommand(newLogsCommands(&sbParams))
	return command
}

type createParams struct {
	numberOfNodes int
	source        string
}

func newCreateCommand(sbParams *sbParams) *cobra.Command {
	createParams := createParams{}
	command := &cobra.Command{
		Use:   "create",
		Short: "Create a new sandbox or start existing one",
		RunE: func(ccmd *cobra.Command, args []string) error {
			dockerClient, err := getDockerClient()
			if err != nil {
				return err
			}
			factory, err := getContainerFactoryForSource(sbParams.fs,
				dockerClient,
				createParams.source)
			if err != nil {
				return err
			}
			return doWithSandbox(sbParams, func(c sandbox.Sandbox) error {
				return c.Create(createParams.numberOfNodes, factory)
			})
		},
	}
	command.Flags().StringVar(&createParams.source, "source",
		filepath.Join(os.Getenv("HOME"), "redpanda"),
		"Source of sandbox redpanda binaries - path to relocatable tarball package")
	command.Flags().IntVarP(&createParams.numberOfNodes, "nodes", "n",
		1, "Number of sandbox nodes to create")
	return command
}

type commandDetails struct {
	use           string
	short         string
	sandboxAction func(sandbox.Sandbox) error
	nodeAction    func(sandbox.Node) error
}

func newSandboxOrNodeCommand(
	sbParams *sbParams, details *commandDetails,
) *cobra.Command {
	var node int
	command := &cobra.Command{
		Use:   details.use,
		Short: details.short,
		RunE: func(ccmd *cobra.Command, args []string) error {
			if node == -1 {
				return doWithSandbox(sbParams, details.sandboxAction)
			}
			return doWithSandboxNode(sbParams, node, details.nodeAction)
		},
	}
	command.Flags().IntVarP(&node, "node",
		"n", -1, "Node id (when absent command will be executed for the whole sandbox)")
	return command
}

func newStartCommand(sbParams *sbParams) *cobra.Command {
	return newSandboxOrNodeCommand(sbParams,
		&commandDetails{
			use:           "start",
			short:         "Start either a single node or whole sandbox",
			sandboxAction: sandbox.Sandbox.Start,
			nodeAction:    sandbox.Node.Start,
		})
}

func newStopCommand(sbParams *sbParams) *cobra.Command {
	return newSandboxOrNodeCommand(sbParams,
		&commandDetails{
			use:           "stop",
			short:         "Stop either a single node or whole sandbox",
			sandboxAction: sandbox.Sandbox.Stop,
			nodeAction:    sandbox.Node.Stop,
		})
}

func newRestartCommand(sbParams *sbParams) *cobra.Command {
	return newSandboxOrNodeCommand(sbParams,
		&commandDetails{
			use:           "restart",
			short:         "Restart either a single node or whole sandbox",
			sandboxAction: sandbox.Sandbox.Restart,
			nodeAction:    sandbox.Node.Restart,
		})
}

func newWipeCommand(sbParams *sbParams) *cobra.Command {
	return newSandboxOrNodeCommand(sbParams,
		&commandDetails{
			use: "wipe-restart",
			short: "Remove data and restart either a single " +
				"node or whole sandbox",
			sandboxAction: sandbox.Sandbox.WipeRestart,
			nodeAction:    sandbox.Node.Wipe,
		})
}

func newDestroyCommand(sbParams *sbParams) *cobra.Command {
	command := &cobra.Command{
		Use:   "destroy",
		Short: "Destroy sandbox",
		RunE: func(ccmd *cobra.Command, args []string) error {
			return doWithSandbox(sbParams, sandbox.Sandbox.Destroy)
		},
	}
	return command
}

func newStatusCommand(sbParams *sbParams) *cobra.Command {
	command := &cobra.Command{
		Use:   "status",
		Short: "Display sandbox status",
		RunE: func(ccmd *cobra.Command, args []string) error {
			sandbox, err := getSandbox(sbParams)
			if err != nil {
				return err
			}
			state, err := sandbox.State()
			if err != nil {
				return err
			}
			printSandboxState(sbParams, state)
			return nil
		},
	}
	return command
}

func newLogsCommands(sbParams *sbParams) *cobra.Command {
	var follow = false
	var tailFlag string
	command := newSandboxOrNodeCommand(sbParams,
		&commandDetails{
			use:   "logs",
			short: "Fetch the logs of a sandbox or single node",
			sandboxAction: func(sb sandbox.Sandbox) error {
				numberOfLines, err := pareseTailFlag(tailFlag)
				if err != nil {
					return err
				}
				return printSandboxLogs(sb, numberOfLines, follow)
			},
			nodeAction: func(node sandbox.Node) error {
				numberOfLines, err := pareseTailFlag(tailFlag)
				if err != nil {
					return err
				}
				return printNodeLogs(node, numberOfLines, follow)
			},
		})

	command.Flags().StringVarP(&tailFlag, "tail", "t", "20",
		"Number of lines to show from the end of the logs")
	command.Flags().BoolVarP(&follow, "follow", "f", false,
		"Follow log output")

	return command
}

func pareseTailFlag(tailFlag string) (int, error) {
	if tailFlag == "all" {
		return 0, nil
	}
	return strconv.Atoi(tailFlag)
}

func printNodeLogs(node sandbox.Node, numberOfLines int, follow bool) error {
	readCloser, err := node.Logs(numberOfLines, follow, false)
	if err != nil {
		return err
	}
	defer readCloser.Close()
	_, err = stdcopy.StdCopy(os.Stdout, os.Stdout, readCloser)
	return err
}

func printSandboxLogs(
	sb sandbox.Sandbox, numberOfLines int, follow bool,
) error {
	outToTerminal := terminal.IsTerminal(int(os.Stdout.Fd()))
	readCloser, err := sb.Logs(numberOfLines, follow, outToTerminal)
	if err != nil {
		return err
	}
	defer readCloser.Close()
	_, err = io.Copy(os.Stdout, readCloser)
	return err
}

func doWithSandbox(
	sbParams *sbParams, action func(sandbox.Sandbox) error,
) error {
	sandbox, err := getSandbox(sbParams)
	if err != nil {
		return err
	}
	return action(sandbox)
}

func doWithSandboxNode(
	sbParams *sbParams, nodeID int, action func(sandbox.Node) error,
) error {
	sandbox, err := getSandbox(sbParams)
	if err != nil {
		return err
	}
	node, err := sandbox.Node(nodeID)
	if err != nil {
		return err
	}
	return action(node)
}

func getSandbox(sbParams *sbParams) (sandbox.Sandbox, error) {
	dockerClient, err := getDockerClient()
	if err != nil {
		return nil, err
	}
	return sandbox.NewSandbox(sbParams.fs, sbParams.sbDir, dockerClient), nil
}

func getDockerClient() (*client.Client, error) {
	dockerClient, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	ctx, cancel := docker.CtxWithDefaultTimeout()
	defer cancel()
	dockerClient.NegotiateAPIVersion(ctx)
	return dockerClient, nil
}

func printSandboxState(sbParams *sbParams, state *sandbox.State) {
	table := ui.NewRpkTable(os.Stdout)
	table.SetHeader([]string{
		"Id",
		"Container IP",
		"RPC Port (Host)",
		"RPC Port",
		"Kafka Port (Host)",
		"Kafka Port",
		"Container ID",
		"Status",
	})
	for _, nodeState := range state.NodeStates {
		table.Append([]string{
			fmt.Sprint(nodeState.ID),
			nodeState.ContainerIP,
			fmt.Sprint(nodeState.HostRPCPort),
			fmt.Sprint(nodeState.RPCPort),
			fmt.Sprint(nodeState.HostKafkaPort),
			fmt.Sprint(nodeState.KafkaPort),
			fmt.Sprintf("%.12s", nodeState.ContainerID),
			nodeState.Status,
		})
	}
	fmt.Println()
	fmt.Println("Redpanda sandbox cluster status")
	fmt.Println()
	table.Render()
}

func getContainerFactoryForSource(
	fs afero.Fs, dockerClient *client.Client, source string,
) (docker.ContainerFactory, error) {
	if strings.HasSuffix(source, "tar.gz") ||
		strings.HasSuffix(source, "tar") ||
		strings.HasSuffix(source, "tar.bz") {
		return docker.NewTarballContainerFactroy(fs, dockerClient, source), nil
	} else {
		return nil, fmt.Errorf("Source '%s' not supported", source)
	}
}

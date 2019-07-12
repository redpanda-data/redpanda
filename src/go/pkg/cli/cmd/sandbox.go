package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	sandbox "vectorized/redpanda/sandbox"
	"vectorized/redpanda/sandbox/docker"

	"github.com/docker/docker/client"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
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
		Short: "Redpanda sandbox",
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
	return command
}

type createParams struct {
	numberOfNodes int
	installDir    string
}

func newCreateCommand(sbParams *sbParams) *cobra.Command {
	createParams := createParams{}
	command := &cobra.Command{
		Use:   "create",
		Short: "Creates redpanda sandbox sandbox",
		RunE: func(ccmd *cobra.Command, args []string) error {
			dockerClient, err := getDockerClient()
			if err != nil {
				return err
			}
			factory := docker.NewGenericContainerFactroy(
				dockerClient, createParams.installDir)
			return doWithSandbox(sbParams, func(c sandbox.Sandbox) error {
				return c.Create(createParams.numberOfNodes, factory)
			})
		},
	}
	command.PersistentFlags().StringVar(&createParams.installDir, "install-dir",
		filepath.Join(os.Getenv("HOME"), "redpanda"),
		"If provided sandox nodes will be based on the docker image created with"+
			"the redpanda installation residing in specified directory")
	command.Flags().IntVarP(&createParams.numberOfNodes, "number-of-nodes", "n",
		1, "Number of nodes in the sandbox sandbox")
	return command
}

type commandDetails struct {
	use           string
	short         string
	sandboxAction func(sandbox.Sandbox) error
	nodeAction    func(sandbox.Sandbox, int) error
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
			short:         "Starts the sandbox or single node within the sandbox",
			sandboxAction: sandbox.Sandbox.Start,
			nodeAction:    sandbox.Sandbox.StartNode,
		})
}

func newStopCommand(sbParams *sbParams) *cobra.Command {
	return newSandboxOrNodeCommand(sbParams,
		&commandDetails{
			use:           "stop",
			short:         "Stops the sandbox or single node within the sandbox",
			sandboxAction: sandbox.Sandbox.Stop,
			nodeAction:    sandbox.Sandbox.StopNode,
		})
}

func newRestartCommand(sbParams *sbParams) *cobra.Command {
	return newSandboxOrNodeCommand(sbParams,
		&commandDetails{
			use:           "restart",
			short:         "Restarts the sandbox or single node within the sandbox",
			sandboxAction: sandbox.Sandbox.Restart,
			nodeAction:    sandbox.Sandbox.RestartNode,
		})
}

func newWipeCommand(sbParams *sbParams) *cobra.Command {
	return newSandboxOrNodeCommand(sbParams,
		&commandDetails{
			use:           "wipe-restart",
			short:         "Wipes and restarts the sandbox or single node within the sandbox",
			sandboxAction: sandbox.Sandbox.WipeRestart,
			nodeAction:    sandbox.Sandbox.WipeRestartNode,
		})
}

func newDestroyCommand(sbParams *sbParams) *cobra.Command {
	command := &cobra.Command{
		Use:   "destroy",
		Short: "Destroys redpanda sandbox",
		RunE: func(ccmd *cobra.Command, args []string) error {
			return doWithSandbox(sbParams, sandbox.Sandbox.Destroy)
		},
	}
	return command
}

func newStatusCommand(sbParams *sbParams) *cobra.Command {
	command := &cobra.Command{
		Use:   "status",
		Short: "Returns sandbox status",
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
	sbParams *sbParams, nodeID int, action func(sandbox.Sandbox, int) error,
) error {
	sandbox, err := getSandbox(sbParams)
	if err != nil {
		return err
	}
	return action(sandbox, nodeID)
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
	table := tablewriter.NewWriter(os.Stdout)
	fmt.Println()
	fmt.Println(os.Executable())
	fmt.Println("Redpanda sandbox cluster status")
	fmt.Println()
	table.SetHeader([]string{
		"Id",
		"Container IP",
		"Forwarded RPC Port",
		"RPC Port",
		"Container ID",
		"Status",
	})
	table.SetRowLine(false)
	for _, nodeState := range state.NodeStates {
		table.Append([]string{
			fmt.Sprint(nodeState.ID),
			nodeState.ContainerIP,
			fmt.Sprint(nodeState.HostRPCPort),
			fmt.Sprint(nodeState.RPCPort),
			fmt.Sprintf("%.12s", nodeState.ContainerID),
			nodeState.Status,
		})
	}
	table.Render()
}

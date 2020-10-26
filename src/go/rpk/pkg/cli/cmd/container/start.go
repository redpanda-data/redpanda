package container

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"vectorized/pkg/cli/cmd/container/common"
	"vectorized/pkg/cli/ui"
	"vectorized/pkg/config"
	"vectorized/pkg/net"

	"github.com/docker/docker/api/types"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func Start(fs afero.Fs) *cobra.Command {
	var (
		nodes uint
	)
	command := &cobra.Command{
		Use:   "start",
		Short: "Start a local container cluster",
		RunE: func(_ *cobra.Command, _ []string) error {
			if nodes < 1 {
				return errors.New(
					"--nodes should be 1 or greater",
				)
			}
			c, err := common.NewDockerClient()
			if err != nil {
				return err
			}
			defer c.Close()

			return common.WrapIfConnErr(startCluster(
				fs,
				c,
				nodes,
			))
		},
	}

	command.Flags().UintVarP(
		&nodes,
		"nodes",
		"n",
		1,
		"The number of nodes to start",
	)

	return command
}

func startCluster(fs afero.Fs, c common.Client, nodes uint) error {
	// Check if cluster exists and start it again.
	restarted, err := restartCluster(fs, c)
	if err != nil {
		return err
	}
	// If a cluster was restarted, there's nothing else to do.
	if restarted {
		return nil
	}

	log.Info("Downloading latest version of Redpanda")
	err = common.PullImage(c)
	if err != nil {
		log.Debugf("Error trying to pull latest image: %v", err)

		msg := "Couldn't pull image and a local one wasn't found either."
		if c.IsErrConnectionFailed(err) {
			msg += "\nPlease check your internet connection" +
				" and try again."
		}

		present, checkErr := common.CheckIfImgPresent(c)

		if checkErr != nil {
			log.Debugf("Error trying to list local images: %v", err)

		}
		if !present {
			return errors.New(msg)
		}
	}

	dir := common.ClusterDir()
	// If it doesn't exist already, create a directory for the cluster.
	exists, err := afero.DirExists(fs, dir)
	if err != nil {
		return err
	}
	if !exists {
		err = fs.MkdirAll(dir, 0755)
		if err != nil {
			return err
		}
	}

	// Create the docker network if it doesn't exist already
	netID, err := common.CreateNetwork(c)
	if err != nil {
		return err
	}

	// Start a seed node.
	seedID := uint(0)
	seedKafkaPort, err := net.GetFreePort()
	if err != nil {
		return err
	}
	seedRPCPort, err := net.GetFreePort()
	if err != nil {
		return err
	}
	seedState, err := common.CreateNode(
		fs,
		c,
		seedID,
		seedKafkaPort,
		seedRPCPort,
		netID,
	)
	if err != nil {
		return err
	}

	seedIP, seedKafkaPort, err := startNode(
		fs,
		c,
		seedID,
		seedKafkaPort,
		seedRPCPort,
		0,
		seedState.ContainerID,
		seedState.ContainerIP,
		"",
	)
	if err != nil {
		return err
	}

	seedAddr := fmt.Sprintf("%s:%d", seedIP, seedKafkaPort)

	addrs := []string{seedAddr}

	mu := sync.Mutex{}

	grp, _ := errgroup.WithContext(context.Background())

	for nodeID := uint(1); nodeID < nodes; nodeID++ {
		id := nodeID
		grp.Go(func() error {
			kafkaPort, err := net.GetFreePort()
			if err != nil {
				return err
			}
			rpcPort, err := net.GetFreePort()
			if err != nil {
				return err
			}
			state, err := common.CreateNode(fs, c, id, kafkaPort, rpcPort, netID)
			if err != nil {
				return err
			}
			log.Debugf(
				"Created container with NodeID=%d, IP=%s, ID='%s",
				id,
				state.ContainerIP,
				state.ContainerID,
			)
			ip, port, err := startNode(
				fs,
				c,
				id,
				kafkaPort,
				rpcPort,
				seedRPCPort,
				state.ContainerID,
				state.ContainerIP,
				seedState.ContainerIP,
			)
			if err != nil {
				return err
			}
			mu.Lock()
			addrs = append(addrs, fmt.Sprintf(
				"%s:%d",
				ip,
				port,
			))
			mu.Unlock()
			return nil
		})
	}

	err = grp.Wait()
	if err != nil {
		return err
	}

	t := ui.NewRpkTable(log.StandardLogger().Out)
	t.SetColWidth(80)
	t.SetAutoWrapText(true)
	t.SetHeader([]string{"Node ID", "Address", "Config"})
	for id, addr := range addrs {
		t.Append([]string{
			fmt.Sprint(id),
			addr,
			common.ConfPath(uint(id)),
		})
	}

	t.Render()
	log.Infof(
		"\nCluster started! You may use 'rpk api' to interact with"+
			" the cluster. E.g:\n\nrpk api status --brokers %s\n",
		seedAddr,
	)
	return nil
}

func restartCluster(fs afero.Fs, c common.Client) (bool, error) {
	// Check if a cluster is running
	nodeIDs, err := common.GetExistingNodes(fs)
	if err != nil {
		return false, err
	}
	// If there isn't an existing cluster, there's nothing to restart.
	if len(nodeIDs) == 0 {
		return false, nil
	}
	grp, _ := errgroup.WithContext(context.Background())
	for _, nodeID := range nodeIDs {
		id := nodeID
		grp.Go(func() error {
			state, err := common.GetState(c, id)
			if err != nil {
				if c.IsErrNotFound(err) {
					msg := "Found data for an existing" +
						" cluster, but the container" +
						" for node %d was removed.\n" +
						"Please run 'rpk container" +
						" purge' to delete all" +
						" remaining data and create" +
						" a new cluster with 'rpk" +
						" container start'."
					return fmt.Errorf(msg, id)
				}
				return err
			}
			if !state.Running {
				ctx, _ := common.DefaultCtx()
				err = c.ContainerStart(
					ctx,
					state.ContainerID,
					types.ContainerStartOptions{},
				)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	err = grp.Wait()
	restarted := len(nodeIDs) > 0 && err == nil
	return restarted, err
}

func startNode(
	fs afero.Fs,
	c common.Client,
	nodeID, kafkaPort, rpcPort, seedRPCPort uint,
	containerID, ip, seedIP string,
) (string, uint, error) {
	conf, err := writeNodeConfig(fs, nodeID, kafkaPort, rpcPort, seedRPCPort, ip, seedIP, common.ConfPath(nodeID))
	if err != nil {
		return "", 0, err
	}
	ctx, _ := common.DefaultCtx()
	err = c.ContainerStart(ctx, containerID, types.ContainerStartOptions{})
	advert := conf.Redpanda.AdvertisedKafkaApi
	return advert.Address, uint(advert.Port), err
}

func writeNodeConfig(
	fs afero.Fs,
	nodeID, kafkaPort, rpcPort, seedRPCPort uint,
	ip, seedIP, path string,
) (*config.Config, error) {
	conf := config.DefaultConfig()
	conf.Redpanda.Id = int(nodeID)

	conf.Rpk.AdditionalStartFlags = []string{"--overprovisioned"}
	conf.Redpanda.DeveloperMode = true

	err := applyPlatformSpecificConf(&conf, kafkaPort, rpcPort, seedRPCPort, ip, seedIP)
	if err != nil {
		return nil, err
	}

	return &conf, config.WriteConfig(fs, &conf, path)
}

package container

import (
	"context"
	"os"
	"vectorized/pkg/cli/cmd/container/common"

	"github.com/docker/docker/api/types"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func Purge(fs afero.Fs) *cobra.Command {
	command := &cobra.Command{
		Use:   "purge",
		Short: "Stop and remove an existing local container cluster's data",
		RunE: func(_ *cobra.Command, _ []string) error {
			c, err := common.NewDockerClient()
			if err != nil {
				return err
			}
			defer c.Close()
			return common.WrapIfConnErr(purgeCluster(fs, c))
		},
	}

	return command
}

func purgeCluster(fs afero.Fs, c common.Client) error {
	nodeIDs, err := common.GetExistingNodes(fs)
	if err != nil {
		return err
	}
	if len(nodeIDs) == 0 {
		log.Info(
			`No nodes to remove.
You may start a new local cluster with 'rpk container start'`,
		)
		return nil
	}
	err = stopCluster(fs, c)
	if err != nil {
		return err
	}
	grp, _ := errgroup.WithContext(context.Background())
	for _, nodeID := range nodeIDs {
		id := nodeID
		grp.Go(func() error {
			err := common.RemoveNodeDir(fs, id)
			if err != nil {
				if !os.IsNotExist(err) {
					return err
				}
			}
			log.Debugf("Deleted data for node %d", id)
			ctx, _ := common.DefaultCtx()
			name := common.Name(id)
			err = c.ContainerRemove(
				ctx,
				name,
				types.ContainerRemoveOptions{
					RemoveVolumes: true,
					Force:         true,
				},
			)
			if err != nil {
				if !c.IsErrNotFound(err) {
					return err
				}
				log.Debug(err)
			}
			log.Debugf("Removed container '%s'", name)
			return nil
		})
	}
	err = grp.Wait()
	if err != nil {
		return err
	}
	err = common.RemoveNetwork(c)
	if err != nil {
		return err
	}
	log.Info("Deleted cluster data.")
	return nil
}

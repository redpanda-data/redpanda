package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"vectorized/redpanda/sandbox/docker/labels"
	"vectorized/utils"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/docker/pkg/term"
	"github.com/docker/go-connections/nat"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

// Docker file constant, will be used as a part of build context,
// the file does not have to be parameterized as the build context
// is controlled totally by the TarballContainerFactory
const dockerFile = `
FROM fedora:30
RUN mkdir -p /opt/redpanda
ADD ./redpanda.tar.gz /opt/redpanda
RUN rm -rf /opt/redpanda/conf

VOLUME /opt/redpanda/data /opt/redpanda/conf
CMD ["/opt/redpanda/bin/redpanda", "--redpanda-cfg", "/opt/redpanda/conf/redpanda.yaml"]
`

func NewTarballContainerFactroy(
	fs afero.Fs, dockerClient *client.Client, tarball string,
) ContainerFactory {
	return &tarballContainerFactory{
		fs:           fs,
		dockerClient: dockerClient,
		tarball:      tarball,
	}
}

const (
	redpandaTarMd5 = "redpanda-tarball-md5"
)

type tarballContainerFactory struct {
	ContainerFactory
	fs           afero.Fs
	dockerClient *client.Client
	tarball      string
}

func (f *tarballContainerFactory) buildImageFromTarball(
	cfg *NodeContainerCfg,
) (string, error) {
	imageName := "v-sandbox"
	// Build docker build ctx in tarball
	md5, err := utils.FileMd5(f.fs, f.tarball)
	ctx, cancel := CtxWithDefaultTimeout()
	defer cancel()
	log.Debugf("Loking for '%s' images with source tarball MD5: '%s'",
		imageName, md5)
	images, err := f.dockerClient.ImageList(ctx,
		types.ImageListOptions{
			Filters: filters.NewArgs(
				filters.KeyValuePair{
					Key:   "label",
					Value: fmt.Sprintf("%s=%s", redpandaTarMd5, md5),
				},
			),
		})

	if err != nil {
		return "", err
	}

	if len(images) != 0 {
		log.Debugf("Image %s with source tarball MD5 '%s' found, skipping build",
			imageName,
			md5)
		return imageName, nil
	}
	buildCtxBuf := new(bytes.Buffer)
	tarWriter := tar.NewWriter(buildCtxBuf)
	rpTarballBytes, err := afero.ReadFile(f.fs, f.tarball)
	if err != nil {
		return "", err
	}
	err = tarWriter.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "redpanda.tar.gz",
		Size:     int64(len(rpTarballBytes)),
	})
	if err != nil {
		return "", err
	}
	_, err = tarWriter.Write(rpTarballBytes)
	if err != nil {
		return "", err
	}
	dockerFileBytes := []byte(dockerFile)
	err = tarWriter.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "Dockerfile",
		Size:     int64(len(dockerFileBytes)),
	})
	if err != nil {
		return "", err
	}
	_, err = tarWriter.Write(dockerFileBytes)
	if err != nil {
		return "", err
	}
	err = tarWriter.Close()
	if err != nil {
		return "", err
	}

	log.Infof("Creating docker image from tarball '%s'", f.tarball)
	buildResp, err := f.dockerClient.ImageBuild(
		context.Background(), buildCtxBuf, types.ImageBuildOptions{
			Tags: []string{imageName},
			Labels: map[string]string{
				redpandaTarMd5: md5,
			},
		})
	if err != nil {
		return "", err
	}
	defer buildResp.Body.Close()
	termFd, isTerm := term.GetFdInfo(os.Stdout)
	jsonmessage.DisplayJSONMessagesStream(buildResp.Body,
		os.Stdout, termFd, isTerm, nil)

	if err != nil {
		return "", err
	}

	return imageName, nil
}

func (f *tarballContainerFactory) CreateNodeContainer(
	cfg *NodeContainerCfg,
) error {
	imageName, err := f.buildImageFromTarball(cfg)
	if err != nil {
		return err
	}
	log.Debugf("Creating container for node '%d' ", cfg.NodeID)

	rpcPort, err := nat.NewPort("tcp", strconv.Itoa(cfg.RPCPort))
	if err != nil {
		return err
	}
	containerConfig := container.Config{
		Image: imageName,
		ExposedPorts: nat.PortSet{
			rpcPort: {},
		},
		Labels: map[string]string{
			labels.ClusterID: cfg.ClusterID,
			labels.NodeID:    strconv.Itoa(cfg.NodeID),
		},
	}

	hostConfig := container.HostConfig{
		Mounts: []mount.Mount{
			mount.Mount{
				Type:     mount.TypeBind,
				Target:   "/opt/redpanda/data",
				Source:   cfg.DataDir,
				ReadOnly: false,
			},
			mount.Mount{
				Type:     mount.TypeBind,
				Target:   "/opt/redpanda/conf",
				Source:   cfg.ConfDir,
				ReadOnly: true,
			},
		},
		PortBindings: nat.PortMap{
			rpcPort: []nat.PortBinding{nat.PortBinding{}},
		},
		CapAdd: []string{"SYS_NICE"},
	}
	networkConfig := network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			cfg.NetworkName: &network.EndpointSettings{
				IPAMConfig: &network.EndpointIPAMConfig{
					IPv4Address: cfg.ContainerIP,
				},
				Aliases: []string{fmt.Sprintf("rp-node-%d", cfg.NodeID)},
			},
		},
	}
	ctx, cancel := CtxWithDefaultTimeout()
	defer cancel()
	body, err := f.dockerClient.ContainerCreate(
		ctx, &containerConfig, &hostConfig, &networkConfig, "")

	log.Debugf("Container with id '%s' created", body.ID)
	return err
}

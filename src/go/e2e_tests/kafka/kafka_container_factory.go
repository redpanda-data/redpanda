package kafka

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"vectorized/pkg/redpanda/sandbox/docker"
	"vectorized/pkg/redpanda/sandbox/docker/labels"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/docker/pkg/term"
	"github.com/docker/go-connections/nat"
	"github.com/gobuffalo/packr"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const (
	kafkaVersionLabel = "kafka-version"
	kafkaVersion      = "0.10.0"
)

const serverProperties = `
broker.id=%d
advertised.listeners=PLAINTEXT://%s
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/tmp/kafka-logs
num.partitions=7
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:2181
zookeeper.connection.timeout.ms=6000
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
`

func NewKafkaContainerFactroy(
	fs afero.Fs, dockerClient *client.Client,
) docker.ContainerFactory {
	return &kafkaContainerFactory{
		fs:           fs,
		dockerClient: dockerClient,
	}
}

type kafkaContainerFactory struct {
	docker.ContainerFactory
	fs           afero.Fs
	dockerClient *client.Client
}

func (f *kafkaContainerFactory) buildImage(
	cfg *docker.NodeContainerCfg,
) (string, error) {
	imageName := "v-kafka-sandbox"
	// Build docker build ctx in tarball
	box := packr.NewBox("./kafka_docker")
	buildCtxBuf := new(bytes.Buffer)
	tarWriter := tar.NewWriter(buildCtxBuf)
	for _, file := range box.List() {
		err := addFileFromBox(file, &box, tarWriter)
		if err != nil {
			return "", err
		}
	}
	srvPropertiesBytes := []byte(renderServerProperties(
		cfg.NodeID,
		fmt.Sprintf("%s:%d", cfg.ContainerIP, cfg.KafkaPort)))
	err := tarWriter.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     "server.properties",
		Mode:     0644,
		Size:     int64(len(srvPropertiesBytes)),
	})
	if err != nil {
		return "", err
	}
	_, err = tarWriter.Write(srvPropertiesBytes)
	if err != nil {
		return "", err
	}

	err = tarWriter.Close()
	if err != nil {
		return "", err
	}

	log.Infof("Creating docker image with Kafka version '%s'", kafkaVersion)
	buildResp, err := f.dockerClient.ImageBuild(
		context.Background(), buildCtxBuf, types.ImageBuildOptions{
			Tags: []string{imageName},
			Labels: map[string]string{
				kafkaVersionLabel: kafkaVersion,
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

func addFileFromBox(file string, box *packr.Box, tarWriter *tar.Writer) error {
	fileBytes, err := box.Find(file)
	if err != nil {
		return err
	}
	mode := int64(0644)
	if strings.HasSuffix(file, ".sh") {
		mode = 0755
	}
	err = tarWriter.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     file,
		Mode:     mode,
		Size:     int64(len(fileBytes)),
	})
	if err != nil {
		return err
	}
	_, err = tarWriter.Write(fileBytes)
	if err != nil {
		return err
	}
	return nil
}

func renderServerProperties(brokerID int, advertisedListerners string) string {
	return fmt.Sprintf(serverProperties, brokerID, advertisedListerners)
}

func (f *kafkaContainerFactory) CreateNodeContainer(
	cfg *docker.NodeContainerCfg,
) error {
	imageName, err := f.buildImage(cfg)
	if err != nil {
		return err
	}
	log.Debugf("Creating container for node '%d' ", cfg.NodeID)

	rpcPort, err := nat.NewPort("tcp", strconv.Itoa(cfg.RPCPort))
	if err != nil {
		return err
	}
	kafkaPort, err := nat.NewPort("tcp", strconv.Itoa(cfg.KafkaPort))
	if err != nil {
		return err
	}
	containerConfig := container.Config{
		Image: imageName,
		ExposedPorts: nat.PortSet{
			rpcPort:   {},
			kafkaPort: {},
		},
		Labels: map[string]string{
			labels.ClusterID: cfg.ClusterID,
			labels.NodeID:    strconv.Itoa(cfg.NodeID),
		},
	}

	hostConfig := container.HostConfig{
		Mounts: []mount.Mount{},
		PortBindings: nat.PortMap{
			rpcPort:   []nat.PortBinding{nat.PortBinding{}},
			kafkaPort: []nat.PortBinding{nat.PortBinding{}},
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
	createCtx, createCancel := docker.CtxWithDefaultTimeout()
	defer createCancel()

	body, err := f.dockerClient.ContainerCreate(
		createCtx, &containerConfig, &hostConfig, &networkConfig, "")
	log.Debugf("Container with id '%s' created", body.ID)
	return err
}

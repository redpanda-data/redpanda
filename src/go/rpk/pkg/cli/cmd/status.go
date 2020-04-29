package cmd

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"vectorized/pkg/api"
	"vectorized/pkg/cli/ui"
	"vectorized/pkg/config"
	"vectorized/pkg/system"

	"github.com/Shopify/sarama"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type status struct {
	metrics  *system.Metrics
	jsonConf string
	topics   []*sarama.TopicMetadata
}

func NewStatusCommand(fs afero.Fs) *cobra.Command {
	var (
		configFile string
		send       bool
		timeout    time.Duration
	)
	command := &cobra.Command{
		Use:          "status",
		Short:        "Check the resource usage in the system, and optionally send it to Vectorized",
		Long:         "",
		SilenceUsage: true,
		RunE: func(ccmd *cobra.Command, args []string) error {
			return executeStatus(fs, configFile, timeout, send)
		},
	}
	command.Flags().StringVar(
		&configFile,
		"config",
		config.DefaultConfig().ConfigFile,
		"Redpanda config file, if not set the file will be searched for"+
			" in the default locations",
	)
	command.Flags().BoolVar(
		&send,
		"send",
		false,
		"Tells `status` whether to send the gathered resource usage data to Vectorized")
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		2000*time.Millisecond,
		"The maximum amount of time to wait for the metrics to be gathered. "+
			"The value passed is a sequence of decimal numbers, each with optional "+
			"fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. "+
			"Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'",
	)
	return command
}

func executeStatus(
	fs afero.Fs, configFile string, timeout time.Duration, send bool,
) error {
	conf, err := config.ReadOrGenerate(fs, configFile)
	if err != nil {
		return err
	}
	if !conf.Rpk.EnableUsageStats {
		log.Warn("Usage stats reporting is disabled, so nothing will" +
			" be sent. To enable it, run" +
			" `rpk config set rpk.enable_usage_stats true`.")
	}
	t := ui.NewRpkTable(log.StandardLogger().Out)
	t.SetColWidth(1000)
	t.SetAutoWrapText(false)
	metrics, errs := system.GatherMetrics(fs, timeout, *conf)
	if len(errs) != 0 {
		for _, err := range errs {
			log.Info("Error gathering metrics: ", err)
		}
	}
	osInfo, err := system.UnameAndDistro(timeout)
	if err != nil {
		log.Info("Error querying OS info: ", err)
	}
	cpuInfo, err := system.CpuInfo()
	if err != nil {
		log.Info("Error querying CPU info: ", err)
	}
	cpuModel := ""
	if len(cpuInfo) > 0 {
		cpuModel = cpuInfo[0].ModelName
	}
	printMetrics(t, metrics, osInfo, cpuModel)

	if conf.Rpk.EnableUsageStats && send {
		if conf.NodeUuid == "" {
			var err error
			conf, err = config.GenerateAndWriteNodeUuid(fs, conf)
			if err != nil {
				log.Info("Error writing the node's UUID: ", err)
			}
		}
		err := sendMetrics(fs, conf, metrics)
		if err != nil {
			log.Info("Error sending metrics: ", err)
		}
	}

	props, err := config.ReadFlat(fs, configFile)
	if err != nil {
		log.Info("Error reading or parsing configuration: ", err)
	} else {
		printConfig(t, props)
	}
	client, err := initClient(
		conf.Redpanda.KafkaApi.Address,
		conf.Redpanda.KafkaApi.Port,
	)
	if err != nil {
		log.Infof("Error initializing redpanda client: %s", err)
		return nil
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Infof("Error initializing redpanda client: %s", err)
		return nil
	}
	defer admin.Close()
	topics, err := topicsDetail(admin)
	if err != nil {
		log.Info("Error fetching the Redpanda topic details: ", err)
	} else if len(topics) > 0 {
		printKafkaInfo(t, client.Brokers(), topics)
	}
	t.Render()

	return nil
}

func printMetrics(
	t *tablewriter.Table, p *system.Metrics, osInfo, cpuModel string,
) {
	t.SetHeader([]string{"Name", "Value"})
	t.Append([]string{"OS", osInfo})
	t.Append([]string{"CPU Model", cpuModel})
	t.Append([]string{"CPU Usage %", fmt.Sprint(p.CpuPercentage)})
	t.Append([]string{"Free Memory (MB)", fmt.Sprint(p.FreeMemoryMB)})
	t.Append([]string{"Free Space  (MB)", fmt.Sprint(p.FreeSpaceMB)})
}

func printConfig(t *tablewriter.Table, conf map[string]string) {
	keys := []string{}
	for k := range conf {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Append([]string{k, conf[k]})
	}
}

func printKafkaInfo(
	t *tablewriter.Table,
	brokers []*sarama.Broker,
	topics []*sarama.TopicMetadata,
) {
	t.SetAutoMergeCells(true)
	type node struct {
		// map[topic-name][]partitions
		leaderParts  map[string][]int
		replicaParts map[string][]int
	}
	nodePartitions := map[int]*node{}
	for _, topic := range topics {
		for _, p := range topic.Partitions {
			leaderID := int(p.Leader)
			n := nodePartitions[leaderID]
			if n != nil {
				topicParts := n.leaderParts[topic.Name]
				topicParts = append(topicParts, int(p.ID))
				if n.leaderParts == nil {
					n.leaderParts = map[string][]int{}
				}
				n.leaderParts[topic.Name] = topicParts
			} else {
				leaderParts := map[string][]int{}
				leaderParts[topic.Name] = []int{int(p.ID)}
				nodePartitions[leaderID] = &node{
					leaderParts: leaderParts,
				}
			}

			for _, r := range p.Replicas {
				replicaID := int(r)
				// Don't list leaders as replicas of their partitions
				if replicaID == leaderID {
					continue
				}
				n := nodePartitions[replicaID]
				if n != nil {
					topicParts := n.replicaParts[topic.Name]
					topicParts = append(topicParts, int(p.ID))
					if n.replicaParts == nil {
						n.replicaParts = map[string][]int{}
					}
					n.replicaParts[topic.Name] = topicParts
				} else {
					replicaParts := map[string][]int{}
					replicaParts[topic.Name] = []int{int(p.ID)}
					nodePartitions[replicaID] = &node{
						replicaParts: replicaParts,
					}
				}
			}
		}
	}
	idToBroker := map[int]sarama.Broker{}
	for _, broker := range brokers {
		if broker != nil {
			idToBroker[int(broker.ID())] = *broker
		}
	}
	nodeIDs := []int{}
	for nodeID, _ := range nodePartitions {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Ints(nodeIDs)
	t.Append([]string{"", ""})
	t.Append([]string{"Redpanda Cluster Status", ""})
	t.Append([]string{"Node ID (IP)", "Partitions"})
	for _, nodeID := range nodeIDs {
		node := nodePartitions[nodeID]
		broker := idToBroker[nodeID]
		nodeInfo := fmt.Sprintf("%d (%s)", nodeID, broker.Addr())
		t.Append([]string{
			nodeInfo,
			"Leader: " + formatTopicsAndPartitions(node.leaderParts),
		})
		t.Append([]string{
			nodeInfo,
			"Replica: " + formatTopicsAndPartitions(node.replicaParts),
		})
	}
}

func sendMetrics(
	fs afero.Fs, conf *config.Config, metrics *system.Metrics,
) error {
	payload := api.MetricsPayload{
		FreeMemoryMB:  metrics.FreeMemoryMB,
		FreeSpaceMB:   metrics.FreeSpaceMB,
		CpuPercentage: metrics.CpuPercentage,
	}
	return api.SendMetrics(payload, *conf)
}

func initClient(ip string, port int) (sarama.Client, error) {
	saramaConf := sarama.NewConfig()
	saramaConf.Version = sarama.V2_4_0_0
	saramaConf.Producer.Return.Successes = true
	saramaConf.Admin.Timeout = 1 * time.Second
	selfAddr := fmt.Sprintf("%s:%d", ip, port)
	return sarama.NewClient([]string{selfAddr}, saramaConf)
}

func topicsDetail(admin sarama.ClusterAdmin) ([]*sarama.TopicMetadata, error) {
	topics, err := admin.ListTopics()
	if err != nil {
		return nil, err
	}
	topicNames := []string{}
	for name, _ := range topics {
		topicNames = append(topicNames, name)
	}
	return admin.DescribeTopics(topicNames)
}

func formatTopicsAndPartitions(tps map[string][]int) string {
	topicNames := []string{}
	for topicName, _ := range tps {
		topicNames = append(topicNames, topicName)
	}
	sort.Strings(topicNames)
	buf := []string{}
	for _, topicName := range topicNames {
		parts := tps[topicName]
		buf = append(buf, formatTopicPartitions(topicName, parts))
	}
	return strings.Join(buf, "; ")
}

func formatTopicPartitions(name string, partitions []int) string {
	strParts := []string{}
	for _, part := range partitions {
		strParts = append(strParts, strconv.Itoa(part))
	}
	return fmt.Sprintf("%s: [%s]", name, strings.Join(strParts, ", "))
}

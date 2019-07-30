package e2e_tests

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"
	"vectorized/e2e_tests/builders"
	"vectorized/e2e_tests/kafka"
	"vectorized/pkg/redpanda/sandbox"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

var (
	brokerType builders.BrokerType
	tarball    string
	verbose    bool
)

func TestMain(m *testing.M) {
	sarama.Logger = log.StandardLogger()
	brokerTypeStr := flag.String(
		"broker-type", "redpanda", "One of [kafka, redpanda]")
	flag.StringVar(&tarball, "tarball", "", "Redpanda tarball path")
	flag.Parse()
	if testing.Verbose() {
		log.SetLevel(log.DebugLevel)
	}
	switch *brokerTypeStr {
	case "redpanda":
		brokerType = builders.RedpandaBroker
	case "kafka":
		brokerType = builders.KafkaBroker
	default:
		log.Fatalf("Unsupported broker type '%s'", *brokerTypeStr)
	}

	os.Exit(m.Run())
}

func createSandbox(nodes int) sandbox.Sandbox {
	sandbox, err := builders.NewSandboxBuilder(afero.NewOsFs()).
		WithNodes(nodes).
		FromTarball(tarball).
		WithBrokerType(brokerType).
		DestroyOldIfExists().
		Build()
	if err != nil {
		log.Fatal("Unable to create redpanda sandbox: ", err)
	}
	return sandbox
}

func Test_SimpleProduceConsume(t *testing.T) {
	sbox := createSandbox(1)
	defer sbox.Destroy()
	kafka.WaitForSandbox(sbox)
	tests := []struct {
		name string
		test func(*testing.T, sandbox.Sandbox)
	}{
		{
			name: "first message published at the topic MUST have an offset 0",
			test: func(t *testing.T, sandbox sandbox.Sandbox) {
				//given
				topic := kafka.CreateRandomTopic(sandbox, 1, 1)
				value := "test message content"
				//when
				producer := kafka.CreateProducer(sbox, sarama.NewConfig())
				p, off, err := producer.SendMessage(kafka.NewProducerMessage(topic, value))
				log.Debugf("Produced partition '%d', offset '%d', err '%s'", p, off, err)
				//then
				consumer := kafka.CreateConsumer(sbox, sarama.NewConfig())
				msgs := kafka.ReadMessagesFromTopic(consumer, 2*time.Second, topic)
				assert.Len(t, msgs, 1, "There should be one message consumed")
				msg := msgs[0]
				assert.Equal(t, int64(0), msg.Offset,
					"First message offset should be equal to 0")
				assert.Equal(t, value, string(msg.Value),
					"Value should be equal to the one that was published")
				assert.Equal(t, p, msg.Partition,
					"Read partition should be equal to the one published")
			},
		},
		{
			name: "at the same partition oreder of publish/consume should be the same",
			test: func(t *testing.T, sandbox sandbox.Sandbox) {
				//given
				topic := kafka.CreateRandomTopic(sandbox, 1, 1)
				numberOfMessages := 5
				//when
				producer := kafka.CreateProducer(sbox, sarama.NewConfig())
				for i := 0; i < numberOfMessages; i++ {
					p, off, err := producer.SendMessage(kafka.NewProducerMessage(topic, fmt.Sprint(i)))
					log.Debugf("Produced partition '%d', offset '%d', err '%v'", p, off, err)
				}
				//then
				consumer := kafka.CreateConsumer(sbox, sarama.NewConfig())
				msgs := kafka.ReadMessagesFromTopic(consumer, 2*time.Second, topic)
				assert.Len(t, msgs, numberOfMessages)
				for i, msg := range msgs {
					assert.Equal(t, fmt.Sprint(i), string(msg.Value))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.test(t, sbox)
		})
	}
}

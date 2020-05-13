package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

func InitClient(ip string, port int) (sarama.Client, error) {
	saramaConf := sarama.NewConfig()
	saramaConf.Version = sarama.V2_4_0_0
	saramaConf.Producer.Return.Successes = true
	saramaConf.Admin.Timeout = 1 * time.Second
	selfAddr := fmt.Sprintf("%s:%d", ip, port)
	return sarama.NewClient([]string{selfAddr}, saramaConf)
}

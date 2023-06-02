package clusterredpandacom

import (
	"context"
	"fmt"

	"github.com/jcmturner/gokrb5/v8/client"
	krbconfig "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/redpanda-data/console/backend/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/k8s/apis/cluster.redpanda.com/v1alpha1"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	ctrl "sigs.k8s.io/controller-runtime"
)

// Reference implementation https://github.com/redpanda-data/console/blob/0ba44b236b6ddd7191da015f44a9302fc13665ec/backend/pkg/kafka/config_helper.go#L44

func newKgoConfig(ctx context.Context, topic *v1alpha1.Topic) ([]kgo.Opt, error) { // nolint:funlen // This is copy of the console
	log := ctrl.LoggerFrom(ctx)
	log.WithName("TopicReconciler.newKgoConfig")

	clientID := "redpanda-operator"
	if topic.Spec.ClientID != nil {
		clientID = *topic.Spec.ClientID
	}

	if len(topic.Spec.KafkaAPISpec.Brokers) == 0 {
		return nil, fmt.Errorf("empty broker list")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(topic.Spec.KafkaAPISpec.Brokers...),
		kgo.ClientID(clientID),
	}

	metricsLabel := "redpanda_operator"
	if topic.Spec.MetricsNamespace != nil && *topic.Spec.MetricsNamespace != "" {
		metricsLabel = *topic.Spec.MetricsNamespace
	}

	hooks := newClientHooks(ctrl.Log, metricsLabel)

	// Create Logger
	kgoLogger := KgoZapLogger{
		logger: ctrl.Log,
	}
	opts = append(opts, kgo.WithLogger(kgoLogger), kgo.WithHooks(hooks))

	// Add Rack Awareness if configured
	if topic.Spec.RackID != nil && *topic.Spec.RackID != "" {
		opts = append(opts, kgo.Rack(*topic.Spec.RackID))
	}

	// Configure SASL
	if topic.Spec.KafkaAPISpec.SASL == nil {
		return opts, nil
	}

	// SASL Plain
	if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismPlain {
		mechanism := plain.Auth{
			User: topic.Spec.KafkaAPISpec.SASL.Username,
			Pass: topic.Spec.KafkaAPISpec.SASL.Password,
		}.AsMechanism()
		opts = append(opts, kgo.SASL(mechanism))
	}

	// SASL SCRAM
	if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismScramSHA256 ||
		topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismScramSHA512 {
		var mechanism sasl.Mechanism
		scramAuth := scram.Auth{
			User: topic.Spec.KafkaAPISpec.SASL.Username,
			Pass: topic.Spec.KafkaAPISpec.SASL.Password,
		}
		if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismScramSHA256 {
			log.V(4).Info("configuring SCRAM-SHA-256 mechanism")
			mechanism = scramAuth.AsSha256Mechanism()
		}
		if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismScramSHA512 {
			log.V(4).Info("configuring SCRAM-SHA-512 mechanism")
			mechanism = scramAuth.AsSha512Mechanism()
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	// OAuth Bearer
	if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismOAuthBearer {
		mechanism := oauth.Auth{
			Token: topic.Spec.KafkaAPISpec.SASL.OAUth.Token,
		}.AsMechanism()
		opts = append(opts, kgo.SASL(mechanism))
	}

	// Kerberos
	if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismGSSAPI {
		log.V(4).Info("configuring SCRAM-SHA-512 mechanism")
		var krbClient *client.Client

		kerbCfg, err := krbconfig.Load(topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.KerberosConfigPath)
		if err != nil {
			return nil, fmt.Errorf("creating kerberos config from specified config (%s) filepath: %w", topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.KerberosConfigPath, err)
		}
		switch topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.AuthType {
		case "USER_AUTH":
			krbClient = client.NewWithPassword(
				topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.Username,
				topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.Realm,
				topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.Password,
				kerbCfg,
				client.DisablePAFXFAST(!topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.EnableFast))
		case "KEYTAB_AUTH":
			ktb, err := keytab.Load(topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.KeyTabPath)
			if err != nil {
				return nil, fmt.Errorf("loading keytab from (%s) key tab path: %w", topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.KeyTabPath, err)
			}
			krbClient = client.NewWithKeytab(
				topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.Username,
				topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.Realm,
				ktb,
				kerbCfg,
				client.DisablePAFXFAST(!topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.EnableFast))
		}
		kerberosMechanism := kerberos.Auth{
			Client:           krbClient,
			Service:          topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.ServiceName,
			PersistAfterAuth: true,
		}.AsMechanism()
		opts = append(opts, kgo.SASL(kerberosMechanism))
	}

	// AWS MSK IAM
	if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismAWSManagedStreamingIAM {
		mechanism := aws.Auth{
			AccessKey:    topic.Spec.KafkaAPISpec.SASL.AWSMskIam.AccessKey,
			SecretKey:    topic.Spec.KafkaAPISpec.SASL.AWSMskIam.SecretKey,
			SessionToken: topic.Spec.KafkaAPISpec.SASL.AWSMskIam.SessionToken,
			UserAgent:    topic.Spec.KafkaAPISpec.SASL.AWSMskIam.UserAgent,
		}.AsManagedStreamingIAMMechanism()
		opts = append(opts, kgo.SASL(mechanism))
	}

	return opts, nil
}

package clusterredpandacom

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/go-logr/logr"
	"github.com/jcmturner/gokrb5/v8/client"
	krbconfig "github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/redpanda-data/console/backend/pkg/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/redpanda-data/redpanda/src/go/k8s/apis/cluster.redpanda.com/v1alpha1"
)

var ErrEmptyBrokerList = errors.New("empty broker list")

// Reference implementation https://github.com/redpanda-data/console/blob/0ba44b236b6ddd7191da015f44a9302fc13665ec/backend/pkg/kafka/config_helper.go#L44

func newKgoConfig(ctx context.Context, cl k8sclient.Client, topic *v1alpha1.Topic, log logr.Logger) ([]kgo.Opt, error) { // nolint:funlen // This is copy of the console
	log.WithName("newKgoConfig")

	if len(topic.Spec.KafkaAPISpec.Brokers) == 0 {
		return nil, ErrEmptyBrokerList
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(topic.Spec.KafkaAPISpec.Brokers...),
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

	if topic.Spec.KafkaAPISpec.SASL != nil {
		var err error
		opts, err = configureSASL(ctx, cl, topic, opts, log)
		if err != nil {
			return nil, err
		}
	}

	// Configure TLS
	var caCertPool *x509.CertPool
	if topic.Spec.KafkaAPISpec.TLS == nil {
		return opts, nil
	}

	// Root CA
	if topic.Spec.KafkaAPISpec.TLS.CaCert != nil {
		ca, err := topic.Spec.KafkaAPISpec.TLS.CaCert.GetValue(ctx, cl, topic.Namespace, "ca.crt")
		if err != nil {
			return nil, fmt.Errorf("failed to read ca certificate secret: %w", err)
		}

		caCertPool = x509.NewCertPool()
		isSuccessful := caCertPool.AppendCertsFromPEM(ca)
		if !isSuccessful {
			log.Info("failed to append ca file to cert pool, is this a valid PEM format?")
		}
	}

	// If configured load TLS cert & key - Mutual TLS
	var certificates []tls.Certificate
	if topic.Spec.KafkaAPISpec.TLS.Cert != nil && topic.Spec.KafkaAPISpec.TLS.Key != nil {
		// 1. Read certificates
		cert, err := topic.Spec.KafkaAPISpec.TLS.Cert.GetValue(ctx, cl, topic.Namespace, "tls.crt")
		if err != nil {
			return nil, fmt.Errorf("failed to read certificate secret: %w", err)
		}

		certData := cert

		key, err := topic.Spec.KafkaAPISpec.TLS.Cert.GetValue(ctx, cl, topic.Namespace, "tls.key")
		if err != nil {
			return nil, fmt.Errorf("failed to read key certificate secret: %w", err)
		}

		keyData := key

		// 2. Check if private key needs to be decrypted. Decrypt it if passphrase is given, otherwise return error
		pemBlock, _ := pem.Decode(keyData)
		if pemBlock == nil {
			return nil, fmt.Errorf("no valid private key found") // nolint:goerr113 // this error will not be handled by operator
		}

		tlsCert, err := tls.X509KeyPair(certData, keyData)
		if err != nil {
			return nil, fmt.Errorf("cannot parse pem: %w", err)
		}
		certificates = []tls.Certificate{tlsCert}
	}

	tlsDialer := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 10 * time.Second},
		Config: &tls.Config{
			//nolint:gosec // InsecureSkipVerify may be true upon user's responsibility.
			InsecureSkipVerify: topic.Spec.KafkaAPISpec.TLS.InsecureSkipTLSVerify,
			Certificates:       certificates,
			RootCAs:            caCertPool,
		},
	}

	return append(opts, kgo.Dialer(tlsDialer.DialContext)), nil
}

func configureSASL(ctx context.Context, cl k8sclient.Client, topic *v1alpha1.Topic, opts []kgo.Opt, log logr.Logger) ([]kgo.Opt, error) { // nolint:funlen // configure SASL is almost a copy from console project
	// SASL Plain
	if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismPlain {
		p, err := topic.Spec.KafkaAPISpec.SASL.Password.GetValue(ctx, cl, topic.Namespace, "password")
		if err != nil {
			return nil, fmt.Errorf("unable to fetch sasl plain password: %w", err)
		}
		mechanism := plain.Auth{
			User: topic.Spec.KafkaAPISpec.SASL.Username,
			Pass: string(p),
		}.AsMechanism()
		opts = append(opts, kgo.SASL(mechanism))
	}

	// SASL SCRAM
	if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismScramSHA256 ||
		topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismScramSHA512 {
		p, err := topic.Spec.KafkaAPISpec.SASL.Password.GetValue(ctx, cl, topic.Namespace, "password")
		if err != nil {
			return nil, fmt.Errorf("unable to fetch sasl scram password: %w", err)
		}
		var mechanism sasl.Mechanism
		scramAuth := scram.Auth{
			User: topic.Spec.KafkaAPISpec.SASL.Username,
			Pass: string(p),
		}
		if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismScramSHA256 {
			log.V(TraceLevel).Info("configuring SCRAM-SHA-256 mechanism")
			mechanism = scramAuth.AsSha256Mechanism()
		}
		if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismScramSHA512 {
			log.V(TraceLevel).Info("configuring SCRAM-SHA-512 mechanism")
			mechanism = scramAuth.AsSha512Mechanism()
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	// OAuth Bearer
	if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismOAuthBearer {
		t, err := topic.Spec.KafkaAPISpec.SASL.OAUth.Token.GetValue(ctx, cl, topic.Namespace, "token")
		if err != nil {
			return nil, fmt.Errorf("unable to fetch token: %w", err)
		}
		mechanism := oauth.Auth{
			Token: string(t),
		}.AsMechanism()
		opts = append(opts, kgo.SASL(mechanism))
	}

	// Kerberos
	if topic.Spec.KafkaAPISpec.SASL.Mechanism == config.SASLMechanismGSSAPI {
		log.V(TraceLevel).Info("configuring SCRAM-SHA-512 mechanism")
		var krbClient *client.Client

		kerbCfg, err := krbconfig.Load(topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.KerberosConfigPath)
		if err != nil {
			return nil, fmt.Errorf("creating kerberos config from specified config (%s) filepath: %w", topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.KerberosConfigPath, err)
		}
		switch topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.AuthType {
		case "USER_AUTH":
			p, err := topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.Password.GetValue(ctx, cl, topic.Namespace, "password")
			if err != nil {
				return nil, fmt.Errorf("unable to fetch sasl gssapi password: %w", err)
			}
			krbClient = client.NewWithPassword(
				topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.Username,
				topic.Spec.KafkaAPISpec.SASL.GSSAPIConfig.Realm,
				string(p),
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
		s, err := topic.Spec.KafkaAPISpec.SASL.AWSMskIam.SecretKey.GetValue(ctx, cl, topic.Namespace, "secret")
		if err != nil {
			return nil, fmt.Errorf("unable to fetch aws msk secret key: %w", err)
		}
		t, err := topic.Spec.KafkaAPISpec.SASL.AWSMskIam.SessionToken.GetValue(ctx, cl, topic.Namespace, "token")
		if err != nil {
			return nil, fmt.Errorf("unable to fetch aws msk secret key: %w", err)
		}
		mechanism := aws.Auth{
			AccessKey:    topic.Spec.KafkaAPISpec.SASL.AWSMskIam.AccessKey,
			SecretKey:    string(s),
			SessionToken: string(t),
			UserAgent:    topic.Spec.KafkaAPISpec.SASL.AWSMskIam.UserAgent,
		}.AsManagedStreamingIAMMechanism()
		opts = append(opts, kgo.SASL(mechanism))
	}
	return opts, nil
}

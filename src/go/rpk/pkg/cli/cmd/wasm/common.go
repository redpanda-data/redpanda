package wasm

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cespare/xxhash"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const coprocTopic = "coprocessor_internal_topic"

func checkCoprocType(coprocType string) error {
	switch coprocType {
	case "async", "data-policy":
		return nil
	default:
		return fmt.Errorf("Unexpected WASM engine type: '%s'", coprocType)
	}
}

func nameIDKey(name string) []byte {
	hash := xxhash.Sum64([]byte(name))
	id := make([]byte, 8)
	binary.LittleEndian.PutUint64(id, hash)
	return id
}

// We always try creating the coproc topic to ensure it exists, creating a
// topic that already exists is fine. We use replicas -1 to ensure we use
// the broker default, which is now three for three+ node clusters.
func ensureCoprocTopic(cl *kgo.Client) error {
	req := kmsg.NewCreateTopicsRequest()
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic = coprocTopic
	t.NumPartitions = 1
	t.ReplicationFactor = -1
	for _, kvs := range [][2]string{
		{"cleanup.policy", "compact"},
		{"compression.type", "zstd"},
	} {
		c := kmsg.NewCreateTopicsRequestTopicConfig()
		c.Name = kvs[0]
		c.Value = kmsg.StringPtr(kvs[1])
		t.Configs = append(t.Configs, c)
	}
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		return err
	}
	err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)

	// nil error = topic did not exist
	// and it's fine that the topic exists
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}
	return nil
}

// Removing uses a record with no value.
func produceRemoveRecord(cl *kgo.Client, name, coprocType string) error {
	rec := kgo.KeySliceRecord(nameIDKey(name), nil)
	rec.Topic = coprocTopic
	rec.Headers = []kgo.RecordHeader{
		{Key: "action", Value: []byte("remove")},
		{Key: "type", Value: []byte(coprocType)},
	}
	return cl.ProduceSync(context.Background(), rec).FirstErr()
}

// Deploying has sha256 header with the sha of the file contents, and the file
// itself as the record value.
func produceDeployRecord(
	cl *kgo.Client, name, description, coprocType string, file []byte,
) error {
	sha := sha256.Sum256(file)
	rec := kgo.KeySliceRecord(nameIDKey(name), nil)
	rec.Topic = coprocTopic
	rec.Value = file
	rec.Headers = []kgo.RecordHeader{
		{Key: "action", Value: []byte("deploy")},
		{Key: "description", Value: []byte(description)},
		{Key: "sha256", Value: sha[:]},
		{Key: "type", Value: []byte(coprocType)},
	}
	return cl.ProduceSync(context.Background(), rec).FirstErr()
}

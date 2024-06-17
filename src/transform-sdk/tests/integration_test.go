/*
* Copyright 2024 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package integration_tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"testing"

	"github.com/hamba/avro"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

var (
	ctx              context.Context     = context.Background()
	container        *redpanda.Container = nil
	adminClient      *AdminAPIClient     = nil
	kafkaClient      *kgo.Client         = nil
	kafkaAdminClient *kadm.Client        = nil
	srClient         *sr.Client          = nil
)

func TestMain(m *testing.M) {
	log.Println("starting Redpanda...")
	// Start container, this is shared for all the tests so that they can run in parallel and be faster.
	c, stop := startRedpanda(ctx)
	container = c
	log.Println("Redpanda started!")

	// Setup admin client
	adminURL, err := container.AdminAPIAddress(ctx)
	if err != nil {
		log.Fatalf("unable to access Admin API Address: %v", err)
	}
	adminClient = NewAdminAPIClient(adminURL)
	for _, logger := range []string{"transform", "wasm"} {
		if err := adminClient.SetLogLevel(ctx, logger, "trace"); err != nil {
			log.Fatalf("unable to set log level: %v", err)
		}
	}

	// Setup broker
	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		log.Fatalf("unable to access Admin API Address: %v", err)
	}
	kgoClient, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
	)
	if err != nil {
		log.Fatalf("unable to create kafka client: %v", err)
	}
	kafkaClient = kgoClient

	kafkaAdminClient = kadm.NewClient(kafkaClient)

	srAddress, err := container.SchemaRegistryAddress(ctx)
	if err != nil {
		log.Fatalf("unable to get schema registry address: %v", err)
	}
	srClient, err = sr.NewClient(sr.URLs(srAddress))
	if err != nil {
		log.Fatalf("unable to create schema registry client: %v", err)
	}
	// Initialize schema registry with a dummy request
	_, err = srClient.SupportedTypes(ctx)
	if err != nil {
		log.Fatalf("unable to make schema registry request: %v", err)
	}

	// Run tests
	exitcode := m.Run()
	kgoClient.Close()
	stop()
	os.Exit(exitcode)
}

func makeClient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	broker, err := container.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	opts = append(opts, kgo.SeedBrokers(broker))
	kgoClient, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	return kgoClient
}

func deployTransform(t *testing.T, metadata TransformDeployMetadata, binary []byte) {
	topics := []string{metadata.InputTopic}
	topics = append(topics, metadata.OutputTopics...)
	_, err := kafkaAdminClient.CreateTopics(ctx, 1, 1, nil, topics...)
	require.NoError(t, err)
	err = adminClient.DeployTransform(ctx, metadata, bytes.NewReader(binary))
	require.NoError(t, err)
}

func TestIdentity(t *testing.T) {
	binary := loadWasmFile(t, "IDENTITY")
	metadata := TransformDeployMetadata{
		Name:         "identity-xform",
		InputTopic:   "foo",
		OutputTopics: []string{"bar"},
	}
	deployTransform(t, metadata, binary)
	r := &kgo.Record{
		Key:   []byte("testing"),
		Value: []byte("niceeeee"),
		Headers: []kgo.RecordHeader{
			{
				Key:   "header-key",
				Value: []byte("header-value"),
			},
		},
	}
	client := makeClient(t, kgo.DefaultProduceTopic(metadata.InputTopic), kgo.ConsumeTopics(metadata.OutputTopics...))
	defer client.Close()
	err := client.ProduceSync(ctx, r).FirstErr()
	require.NoError(t, err)
	fetches := client.PollFetches(ctx)
	requireRecordsEquals(t, fetches, r)
}

type logValue struct {
	IntValue    int    `json:"intValue"`
	StringValue string `json:"stringValue"`
}

func (av logValue) MarshalJSON() ([]byte, error) {
	if len(av.StringValue) > 0 {
		return json.Marshal(map[string]string{"stringValue": av.StringValue})
	}
	return json.Marshal(map[string]int{"intValue": av.IntValue})
}

type logAttribute struct {
	Key   string   `json:"key"`
	Value logValue `json:"value"`
}

type openTelemetryLogEvent struct {
	Body           logValue       `json:"body"`
	TimeUnixNano   uint64         `json:"timeUnixNano"`
	SeverityNumber int            `json:"severityNumber"`
	Attributes     []logAttribute `json:"attributes"`
}

func TestLogging(t *testing.T) {
	binary := loadWasmFile(t, "LOGGING")
	metadata := TransformDeployMetadata{
		Name:         "logging-xform",
		InputTopic:   "events",
		OutputTopics: []string{"empty"},
	}
	deployTransform(t, metadata, binary)
	r := &kgo.Record{
		Key:   []byte("testing"),
		Value: []byte("hello, world"),
	}
	client := makeClient(t, kgo.DefaultProduceTopic(metadata.InputTopic), kgo.ConsumeTopics("_redpanda.transform_logs"))
	defer client.Close()
	err := client.ProduceSync(ctx, r).FirstErr()
	require.NoError(t, err)
	fetches := client.PollFetches(ctx)
	require.NoError(t, fetches.Err())
	records := fetches.Records()
	require.Equal(t, 1, len(records), "expected a single log record")
	require.Equal(t, []byte(metadata.Name), records[0].Key)
	var logEvent openTelemetryLogEvent
	require.NoError(t, json.Unmarshal(records[0].Value, &logEvent))
	require.Equal(t, "testing:hello, world\n", logEvent.Body.StringValue)
	require.Equal(t, 13, logEvent.SeverityNumber)
	require.Equal(t, []logAttribute{
		{Key: "transform_name", Value: logValue{StringValue: metadata.Name}},
		{Key: "node", Value: logValue{IntValue: 0}},
	}, logEvent.Attributes)
}

func TestMultipleOutputs(t *testing.T) {
	binary := loadWasmFile(t, "TEE")
	metadata := TransformDeployMetadata{
		Name:         "tee-xform",
		InputTopic:   "zam",
		OutputTopics: []string{"bam", "baz", "qux", "thud", "wham"},
	}
	deployTransform(t, metadata, binary)
	r := &kgo.Record{
		Key:   []byte("testing"),
		Value: []byte("niceeeee"),
		Headers: []kgo.RecordHeader{
			{
				Key:   "header-key",
				Value: []byte("header-value"),
			},
		},
	}
	client := makeClient(t, kgo.DefaultProduceTopic(metadata.InputTopic), kgo.ConsumeTopics(metadata.OutputTopics...))
	defer client.Close()
	err := client.ProduceSync(ctx, r).FirstErr()
	require.NoError(t, err)
	outputs := map[string]bool{}
	for _, topic := range metadata.OutputTopics {
		outputs[topic] = true
	}
	for len(outputs) > 0 {
		fetches := client.PollFetches(ctx)
		for _, got := range fetches.Records() {
			require.Contains(t, outputs, got.Topic, "record found in unexpected topic: %q", got.Topic)
			delete(outputs, got.Topic)
			requireRecordEquals(t, got, r, "record topic mismatch: %q", got.Topic)
		}
	}
}

type (
	RecordV1 struct {
		A int64  `avro:"a" json:"a"`
		B string `avro:"b" json:"b"`
	}
	RecordV2 struct {
		A int64  `avro:"a" json:"a"`
		B string `avro:"b" json:"b"`
		C string `avro:"c" json:"c"`
	}
)

var (
	RecordV1Schema = avro.MustParse(`{
		"type": "record",
		"name": "Example",
		"fields": [
			{"name": "a", "type": "long", "default": 0},
			{"name": "b", "type": "string", "default": ""}
		]
	}`)
	RawRecordV2Schema = `{
		"type": "record",
		"name": "Example",
		"fields": [
			{"name": "a", "type": "long", "default": 0},
			{"name": "b", "type": "string", "default": ""},
			{"name": "c", "type": "string", "default": ""}
		]
	}`
	RecordV2Schema = avro.MustParse(RawRecordV2Schema)
)

func prependSchemaID(data []byte, id int) []byte {
	return append(binary.BigEndian.AppendUint32([]byte{0x0}, uint32(id)), data...)
}

func TestSchemaRegistry(t *testing.T) {
	compat := avro.NewSchemaCompatibility()
	require.NoError(t, compat.Compatible(RecordV2Schema, RecordV1Schema))
	require.NoError(t, compat.Compatible(RecordV1Schema, RecordV2Schema))
	wasmBinary := loadWasmFile(t, "SCHEMA_REGISTRY")
	metadata := TransformDeployMetadata{
		Name:         "sr-xform",
		InputTopic:   "avro",
		OutputTopics: []string{"json"},
	}
	deployTransform(t, metadata, wasmBinary)
	v1 := RecordV1{
		A: 412342,
		B: "aldskjal",
	}
	v1Avro, err := avro.Marshal(RecordV1Schema, &v1)
	require.NoError(t, err)
	r := &kgo.Record{
		Key:   []byte("testing"),
		Value: prependSchemaID(v1Avro, 1),
	}
	client := makeClient(t, kgo.DefaultProduceTopic(metadata.InputTopic), kgo.ConsumeTopics(metadata.OutputTopics...))
	defer client.Close()
	err = client.ProduceSync(ctx, r).FirstErr()
	require.NoError(t, err)
	fetches := client.PollFetches(ctx)
	v1Json, err := json.Marshal(&v1)
	require.NoError(t, err)
	requireRecordsEquals(t, fetches, &kgo.Record{
		Key:     []byte("testing"),
		Value:   v1Json,
		Headers: []kgo.RecordHeader{},
	})
	// Make sure the schema was created by the transform
	schema, err := srClient.SchemaByID(ctx, 1)
	require.NoError(t, err)

	// Quick check that the transform created an additional subject
	// by applying topic name strategy to the input topic
	subj_schema, err := srClient.SchemaByVersion(ctx, "avro-value", -1)
	require.NoError(t, err)
	require.Equal(t, schema, subj_schema.Schema, "Schemas should be the same")

	// Ensure the canonicalized schema is what we expect.
	require.Equal(t, avro.MustParse(schema.Schema).String(), RecordV1Schema.String())
	v2 := RecordV2{
		A: 412342,
		B: "aldskjal",
		C: "zlxkjals",
	}
	// Make sure the transform can handle new schemas
	schemaWithId, err := srClient.CreateSchema(ctx, "demo-topic-value", sr.Schema{
		Schema: RawRecordV2Schema,
		Type:   sr.TypeAvro,
	})
	require.NoError(t, err)
	v2Avro, err := avro.Marshal(RecordV2Schema, &v2)
	r = &kgo.Record{
		Key:   []byte("test2"),
		Value: prependSchemaID(v2Avro, schemaWithId.ID),
	}
	err = client.ProduceSync(ctx, r).FirstErr()
	require.NoError(t, err)
	fetches = client.PollFetches(ctx)
	// Because the transform doesn't know about the new schema it will skip C and we get the same data as v1
	requireRecordsEquals(t, fetches, &kgo.Record{
		Key:     []byte("test2"),
		Value:   v1Json,
		Headers: []kgo.RecordHeader{},
	})
}

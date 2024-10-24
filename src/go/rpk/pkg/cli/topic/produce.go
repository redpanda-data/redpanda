// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/serde"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

func newProduceCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		key        string
		recHeaders []string
		partition  int32

		inFormat        string
		outFormat       string
		compression     string
		acks            int
		maxMessageBytes int32

		tombstone              bool
		allowAutoTopicCreation bool

		schemaIDFlag    string
		keySchemaIDFLag string
		protoFQN        string
		protoKeyFQN     string

		timeout time.Duration
	)

	cmd := &cobra.Command{
		Use:   "produce [TOPIC]",
		Short: "Produce records to a topic",
		Long:  helpProduce,
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			// A few of our flags require up front handling before
			// the kgo client is initialized: compression, acks,
			// retries, and partition.
			opts := []kgo.Opt{
				kgo.ProduceRequestTimeout(5 * time.Second),
			}
			switch compression {
			case "none":
				opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
			case "gzip":
				opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
			case "snappy":
				opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
			case "lz4":
				opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
			case "zstd":
				opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
			default:
				out.Die("invalid compression codec %q", compression)
			}

			if allowAutoTopicCreation {
				opts = append(opts, kgo.AllowAutoTopicCreation())
			}

			switch acks {
			case -1:
				opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
			case 0:
				opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
			case 1:
				opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
			default:
				out.Die("invalid acks %d, only -1, 0, and 1 are supported", acks)
			}

			switch {
			case timeout == 0:
			case timeout < time.Second:
				out.Die("invalid --delivery-timeout less than 1s")
			default:
				opts = append(opts, kgo.RecordDeliveryTimeout(timeout))
				opts = append(opts, kgo.ProduceRequestTimeout(timeout+5*time.Second))
			}
			if partition >= 0 {
				opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
			}
			if maxMessageBytes >= 0 {
				opts = append(opts, kgo.ProducerBatchMaxBytes(maxMessageBytes))
				opts = append(opts, kgo.BrokerMaxWriteBytes(2*maxMessageBytes))
			}
			var defaultTopic string
			if len(args) == 1 {
				defaultTopic = args[0]
				opts = append(opts, kgo.DefaultProduceTopic(defaultTopic))
			}
			if len(inFormat) == 0 {
				out.Die("invalid empty format")
			}

			keySchemaID, isKeyTopicName, err := parseSchemaIDFlag(keySchemaIDFLag)
			out.MaybeDie(err, "unable to parse '--schema-key-id' flag: %v", err)
			schemaID, isValTopicName, err := parseSchemaIDFlag(schemaIDFlag)
			out.MaybeDie(err, "unable to parse '--schema-id' flag: %v", err)

			isSchemaRegistry := isKeyTopicName || isValTopicName || keySchemaID >= 0 || schemaID >= 0
			// If the user is using schema registry, we want to use the json
			// format: %v{json} and %k{json}.
			if isSchemaRegistry {
				// Replace any instance of %k or %v with %k{json} or %v{json}.
				inFormat = regexp.MustCompile("%(v|k)([^{]|$)").ReplaceAllString(inFormat, "%$1{json}$2")
			}

			// Parse our input/output formats.
			inf, err := kgo.NewRecordReader(os.Stdin, inFormat)
			out.MaybeDie(err, "unable to parse input format: %v", err)
			var outf *kgo.RecordFormatter
			var outfBuf []byte
			if outFormat != "" {
				outf, err = kgo.NewRecordFormatter(outFormat)
				out.MaybeDie(err, "unable to parse output success format: %v", err)
			}

			// Parse input headers using parseKVs.
			kvs, err := parseKVs(recHeaders)
			out.MaybeDie(err, "unable to parse input headers: %v", err)
			headers := make([]kgo.RecordHeader, 0, len(kvs))
			for k, v := range kvs {
				headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
			}

			// We are now ready to produce.
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, opts...)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()
			defer cl.Flush(context.Background())

			var defaultKeySerde, defaultValSerde *serde.Serde
			var srCl *sr.Client
			if isSchemaRegistry {
				srCl, err = schemaregistry.NewClient(fs, p)
				out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

				keySchema, valSchema, err := querySchemas(cmd.Context(), srCl, keySchemaID, schemaID)
				out.MaybeDie(err, "unable to query schemas from the registry: %v", err)

				if keySchema != nil {
					defaultKeySerde, err = serde.NewSerde(cmd.Context(), srCl, keySchema, keySchemaID, protoKeyFQN)
					out.MaybeDie(err, "unable to build serializer for the key schema: %v", err)
				}
				if valSchema != nil {
					defaultValSerde, err = serde.NewSerde(cmd.Context(), srCl, valSchema, schemaID, protoFQN)
					out.MaybeDie(err, "unable to build serializer for the value schema: %v", err)
				}
			}

			// This is just the cache for TopicName strategy
			// [topicName-key|value]:serde.
			serdeCache := make(map[string]*serde.Serde)
			for {
				r := &kgo.Record{
					Partition: partition,
					Headers:   headers,
				}
				if len(key) > 0 {
					r.Key = []byte(key)
				}
				if err := inf.ReadRecordInto(r); err != nil {
					if !errors.Is(err, io.EOF) {
						fmt.Fprintf(os.Stderr, "record read error: %v\n", err)
					}
					return
				}
				if r.Topic == "" && defaultTopic == "" {
					out.Die("topic to produce to is missing, check --help for produce syntax")
				}
				topicName := defaultTopic
				if topicName == "" {
					topicName = r.Topic
				}
				if len(r.Value) == 0 {
					if tombstone {
						// `null` value
						r.Value = nil
					} else {
						// empty byte-slice
						r.Value = []byte{}
					}
				}

				if defaultKeySerde != nil || isKeyTopicName {
					keySerde := defaultKeySerde
					if isKeyTopicName && keySerde == nil {
						keySerde, err = serdeFromTopicName(cmd.Context(), srCl, topicName, "key", protoKeyFQN, serdeCache)
						if err != nil {
							fmt.Fprintf(os.Stderr, "unable to build key serializer using TopicNameStrategy for topic %q: %v\n", topicName, err)
							return
						}
					}
					record, err := keySerde.EncodeRecord(r.Key)
					out.MaybeDie(err, "unable to encode key: %v", err)
					r.Key = record
				}
				if defaultValSerde != nil || isValTopicName {
					valSerde := defaultValSerde
					if isValTopicName && valSerde == nil {
						valSerde, err = serdeFromTopicName(cmd.Context(), srCl, topicName, "value", protoFQN, serdeCache)
						if err != nil {
							fmt.Fprintf(os.Stderr, "unable to build value serializer using TopicNameStrategy for topic %q: %v\n", topicName, err)
							return
						}
					}
					record, err := valSerde.EncodeRecord(r.Value)
					out.MaybeDie(err, "unable to encode value: %v", err)
					r.Value = record
				}
				cl.Produce(context.Background(), r, func(r *kgo.Record, err error) {
					if isTooLarge := errors.Is(err, kerr.MessageTooLarge); isTooLarge {
						err = fmt.Errorf("%w Check --max-message-bytes flag or your cluster property 'kafka_batch_max_bytes'", err)
					}
					out.MaybeDie(err, "unable to produce record: %v", err)
					if outf != nil {
						outfBuf = outf.AppendRecord(outfBuf[:0], r)
						os.Stdout.Write(outfBuf)
					}
				})
			}
		},
	}

	// The following flags require parsing before we initialize our client.
	cmd.Flags().StringVarP(&compression, "compression", "z", "snappy", "Compression to use for producing batches (none, gzip, snappy, lz4, zstd)")
	cmd.Flags().IntVar(&acks, "acks", -1, "Number of acks required for producing (-1=all, 0=none, 1=leader)")
	cmd.Flags().DurationVar(&timeout, "delivery-timeout", 0, "Per-record delivery timeout, if non-zero, min 1s")
	cmd.Flags().Int32VarP(&partition, "partition", "p", -1, "Partition to directly produce to, if non-negative (also allows %p parsing to set partitions)")
	cmd.Flags().Int32Var(&maxMessageBytes, "max-message-bytes", -1, "If non-negative, maximum size of a record batch before compression")

	cmd.Flags().StringVarP(&inFormat, "format", "f", "%v\n", "Input record format")
	cmd.Flags().StringVarP(&outFormat, "output-format", "o", "Produced to partition %p at offset %o with timestamp %d.\n", "what to write to stdout when a record is successfully produced")
	cmd.Flags().StringArrayVarP(&recHeaders, "header", "H", nil, "Headers in format key:value to add to each record (repeatable)")
	cmd.Flags().StringVarP(&key, "key", "k", "", "A fixed key to use for each record (parsed input keys take precedence)")
	cmd.Flags().BoolVarP(&tombstone, "tombstone", "Z", false, "Produce empty values as tombstones")
	cmd.Flags().BoolVar(&allowAutoTopicCreation, "allow-auto-topic-creation", false, "Auto-create non-existent topics; requires auto_create_topics_enabled on the broker")
	cmd.Flags().StringVar(&schemaIDFlag, "schema-id", "", "Schema ID to encode the record value with, use 'topic' for TopicName strategy")
	cmd.Flags().StringVar(&keySchemaIDFLag, "schema-key-id", "", "Schema ID to encode the record key with, use 'topic' for TopicName strategy")
	cmd.Flags().StringVar(&protoFQN, "schema-type", "", "Name of the protobuf message type to be used to encode the record value using schema registry")
	cmd.Flags().StringVar(&protoKeyFQN, "schema-key-type", "", "Name of the protobuf message type to be used to encode the record key using schema registry")

	// Deprecated
	cmd.Flags().IntVarP(new(int), "num", "n", 1, "")
	cmd.Flags().MarkDeprecated("num", "Invoke rpk multiple times if you wish to repeat records")
	cmd.Flags().BoolVarP(new(bool), "jvm-partitioner", "j", false, "")
	cmd.Flags().MarkDeprecated("jvm-partitioner", "The default is now the jvm-partitioner")
	cmd.Flags().StringVarP(new(string), "timestamp", "t", "", "")
	cmd.Flags().MarkDeprecated("timestamp", "Record timestamps are set when producing")

	return cmd
}

func querySchemas(ctx context.Context, cl *sr.Client, keySchemaID, valSchemaID int) (keySchema *sr.Schema, valSchema *sr.Schema, err error) {
	if keySchemaID > 0 {
		keySch, err := cl.SchemaByID(ctx, keySchemaID)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to retrieve the key schema with ID %v: %v", keySchemaID, err)
		}
		keySchema = &keySch
	}
	if valSchemaID > 0 {
		vSch, err := cl.SchemaByID(ctx, valSchemaID)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to retrieve the value schema with ID %v: %v", valSchemaID, err)
		}
		valSchema = &vSch
	}
	return
}

func parseSchemaIDFlag(flag string) (int, bool, error) {
	if flag == "" {
		return -1, false, nil
	}
	if strings.ToLower(flag) == "topic" {
		return -1, true, nil
	}
	parsed, err := strconv.Atoi(flag)
	if err != nil {
		return -1, false, fmt.Errorf("unable to parse %q: %v; use either a number or 'topic'", flag, err)
	}
	return parsed, false, nil
}

// serdeFromTopicName returns a new serde.Serde based on the topicName strategy.
// First, it searches if the serde is cached, if not, it will query the latest
// schema with subject name: <topic>-<suffix> and cache the result.
func serdeFromTopicName(ctx context.Context, cl *sr.Client, topic, suffix, protoFQN string, serdeCache map[string]*serde.Serde) (*serde.Serde, error) {
	var newSerde *serde.Serde
	schSubjectName := topic + "-" + suffix
	if s, ok := serdeCache[schSubjectName]; ok {
		newSerde = s
	} else {
		schema, err := cl.SchemaByVersion(ctx, schSubjectName, -1)
		if err != nil {
			return nil, fmt.Errorf("unable to get schema with name %q using TopicName strategy: %v", schSubjectName, err)
		}
		newSerde, err = serde.NewSerde(ctx, cl, &schema.Schema, schema.ID, protoFQN)
		if err != nil {
			return nil, fmt.Errorf("unable to build serializer for the schema: %v", err)
		}
		serdeCache[schSubjectName] = newSerde
	}
	return newSerde, nil
}

const helpProduce = `Produce records to a topic.

Producing records reads from STDIN, parses input according to --format, and
produce records to Redpanda. The input formatter understands a wide variety of
formats.

Parsing input operates on either sizes or on delimiters, both of which can be
specified in the same formatting options. If using sizes to specify something,
the size must come before what it is specifying. Delimiters match on an exact
text basis. This command will quit with an error if any input fails to match
your specified format.

Slashes can be used for common escapes:

    \t \n \r \\ \xNN

matches tabs, newlines, carriage returns, slashes, and hex encoded characters.

Percent encoding reads into specific values of a record:

    %t    topic
    %T    topic length
    %k    key
    %K    key length
    %v    value
    %V    value length
    %h    begin the header specification
    %H    number of headers
    %p    partition (if using the --partition flag)

Three escapes exist to parse characters that are used to modify the previous
escapes:

    %%    percent sign
    %{    left brace
    %}    right brace

MODIFIERS

Text and numbers can be read in multiple formats, and the default format can be
changed within brace modifiers. %v reads a value, while %v{hex} reads a value
and then hex decodes it before producing. %T reads the length of a topic from
the input, while %T{3} reads exactly three bytes for a topic from the input.

All modifiers go within braces following a percent-escape.

NUMBERS

Reading number values can have the following modifiers:

     ascii       parse numeric digits until a non-numeric (default)

     hex64       sixteen hex characters
     hex32       eight hex characters
     hex16       four hex characters
     hex8        two hex characters
     hex4        one hex character

     big64       eight byte big endian number
     big32       four byte big endian number
     big16       two byte big endian number
     big8        alias for byte

     little64    eight byte little endian number
     little32    four byte little endian number
     little16    two byte little endian number
     little8     alias for byte

     byte        one byte number
     <digits>    directly specify the length as this many digits
     bool        read "true" as 1, "false" as 0

When reading number sizes, the size corresponds to the size of the encoded
values, not the decoded values. "%T{6}%t{hex}" will read six hex bytes and
decode into three.

TEXT

Reading text values can have the following modifiers:

    hex       read text then hex decode it
    base64    read text then std-encoding base64 decode it
    re        read text matching a regular expression
    json      read text as json then compact it

HEADERS

Headers are parsed with an internal key / value specifier format. For example,
the following will read three headers that begin and end with a space and are
separated by an equal:

    %H{3}%h{ %k=%v }

SCHEMA-REGISTRY

Records can be encoded using a specified schema from our schema registry. Use
the '--schema-id' or '--schema-key-id' flags to define the schema ID, rpk will
retrieve the schemas and encode the record accordingly.

Additionally, utilizing 'topic' in the mentioned flags allows for the use of the
Topic Name Strategy. This strategy identifies a schema subject name based on the
topic itself. For example:

Produce to 'foo', encode using the latest schema in the subject 'foo-value':
    rpk topic produce foo --schema-id=topic

For protobuf schemas, you can specify the fully qualified name of the message
you want the record to be encoded with. Use the 'schema-type' flag or
'schema-key-type'. If the schema contains only one message, specifying the
message name is unnecessary. For example:

Produce to 'foo', using schema ID 1, message FQN Person.Name
    rpk topic produce foo --schema-id 1 --schema-type Person.Name

TOMBSTONES

By default, records produced without a value will have an empty-string value, "".
The below example produces a record with the key 'not_a_tombstone_record' and the
value "":
    rpk topic produce foo -k not_a_tombstone_record
    [ret]

Tombstone records (records with a 'null' value) can be produced by using the '-Z'
flag and creating empty-string value records. Using the same example from above,
but adding the '-Z' flag will produce a record with the key 'tombstone_record'
and the value 'null':
    rpk topic produce foo -k tombstone_record -Z
    [ret]

It is important to note that records produced with values of string "null" are
not considered tombstones by Redpanda.

EXAMPLES

In the below examples, we can parse many records at once. The produce command
reads input and tokenizes based on your specified format. Every time the format
is completely matched, a record is produced and parsing begins anew.

A key and value, separated by a space and ending in newline:
    -f '%k %v\n'
A four byte topic, four byte key, and four byte value:
    -f '%T{4}%K{4}%V{4}%t%k%v'
A value to a specific partition, if using a non-negative --partition flag:
    -f '%p %v\n'
A big-endian uint16 key size, the text " foo ", and then that key:
    -f '%K{big16} foo %k'
A value that can be two or three characters followed by a newline:
    -f '%v{re#...?#}\n'
A key and a json value, separated by a space:
    -f '%k %v{json}'

MISC

Producing requires a topic to produce to. The topic can be specified either
directly on as an argument, or in the input text through %t. A parsed topic
takes precedence over the default passed in topic. If no topic is specified
directly and no topic is parsed, this command will quit with an error.

The input format can parse partitions to produce directly to with %p. Doing so
requires specifying a non-negative --partition flag. Any parsed partition
takes precedence over the --partition flag; specifying the flag is the main
requirement for being able to directly control which partition to produce to.

You can also specify an output format to write when a record is produced
successfully. The output format follows the same formatting rules as the topic
consume command. See that command's help text for a detailed description.
`

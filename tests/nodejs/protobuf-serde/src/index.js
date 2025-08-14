import { create } from "@bufbuild/protobuf";
import { UserSchema } from "./example_pb.js";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { SchemaRegistryClient, ProtobufSerializer, SerdeType, ProtobufDeserializer } from "@confluentinc/schemaregistry";
import { parseArgs } from 'node:util';

const { values: { brokers, sr } } = parseArgs({
  options: {
    brokers: {
      type: 'string',
      description: 'The seed brokers',
    },
    sr: {
      type: 'string',
      description: 'schema registry url',
    },
  },
  allowPositionals: false,
});

const schemaRegistryClient = new SchemaRegistryClient({
  baseURLs: [sr],
});

const kafka = new KafkaJS.Kafka({
  kafkaJS: { brokers: [brokers] },
});

const admin = kafka.admin()
await admin.connect();
await admin.createTopics({ topics: [{ topic: "my-node-topic", numPartitions: 1 }] });
await admin.disconnect();

const homer = create(UserSchema, {
  firstName: "Homer",
  active: true,
  manager: {
    lastName: "Burns",
  },
});

const serializer = new ProtobufSerializer(schemaRegistryClient, SerdeType.VALUE, { autoRegisterSchemas: true })
serializer.registry.add(UserSchema);
const deserializer = new ProtobufDeserializer(schemaRegistryClient, SerdeType.VALUE, {})

const record = { value: await serializer.serialize("my-node-topic", homer) };

const producer = kafka.producer();

await producer.connect();

const [recordMetadata] = await producer.send({
  topic: "my-node-topic",
  messages: [record],
});

const producedOffset = Number.parseInt(recordMetadata.baseOffset);

await producer.disconnect();

const consumer = kafka.consumer({ kafkaJS: { groupId: "my-node-consumer-group", fromBeginning: true } });

await consumer.connect();
await consumer.subscribe({ topic: "my-node-topic" });

let resolve = () => { };
let reject = () => { };
const promise = new Promise((res, rej) => { resolve = res; reject = rej; })

const handle = setTimeout(() => {
  reject(new Error("Timeout waiting for message"));
}, 60_000);

consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const decodedMessage = {
      ...message,
      value: await deserializer.deserialize(topic, message.value)
    };
    console.log("recieved message:", JSON.stringify({
      topic,
      partition,
      message: decodedMessage,
    }, null, 2));
    const offset = Number.parseInt(message.offset)
    if (offset >= producedOffset) {
      console.log("Message received successfully, resolving promise");
      resolve();
    }
  }
});

await promise;

await consumer.disconnect();

clearTimeout(handle);

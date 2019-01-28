# CPP Client

## Producer

Our producer exposes a `create_txn()` api for creating a temporary object.
This object holds a pointer to the client. After a call to `submit()`
the temporary `txn` object becomes invalid.

Our API is simpler, because our product is simpler, we only have
*one* mode of operation. It is also different than databases in that
there is never really any conflicts. This can only fail in case 
of bad network conditions really, or bad cluster conditions, 
but there are no `data races` as such. 

```cpp
 
    // asume *one* api for push-pull
    
    api_ = std::make_unique<api::client>(std::move(co));
```

```cpp

auto txn = api_ -> create_txn();
for (auto n = 0u; n < batch; ++n) {
  auto k = rand_.next_alphanum(kz);
  auto v = rand_.next_str(vz);
  txn.stage(k.data(), k.size(), v.data(), v.size());
}
return txn.submit().then([](auto r) { ... });

```

## Consumer 

Our `consume()` call load balances across all the partitions of the topic.
Each topic partition gets load balanced in a jump_consistent_hash fashion.


```cpp 
seastar::future<>
consumer_forever_example() {
  return seastar::keep_doing([this] {
    return api_->consume().then([](auto r) {
      // process your reply! :)
    });
  });
}
```

# Alternatives
## Kafka

### Producer

```java 

Producer<byte[], byte[]>
    producer = new KafkaProducer<>(producerConfig);

producer.initTransactions(); //initiate transactions
try {
  producer.beginTransaction();  // begin transactions
  for (int i = 0; i < noOfMessages; i++) {
    producer.send(new ProducerRecord<byte[], byte[]>(topic, getKey(i),
                                                     getEvent(messageType, i)));

    try {
      Thread.sleep(delay);
    } catch (InterruptedException e) {}
  }
  producer.commitTransaction();  // commit

} catch (KafkaException e) {
  // For all other exceptions, just abort the transaction and try again.
  producer.abortTransaction();
}
```

### Consumer 

```java

KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig);
consumer.subscribe(Collections.singletonList(topic));

while (true) {
  ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
  for (ConsumerRecord<byte[], byte[]> record : records) {
    System.out.printf("Received Message topic =%s, partition =%s, offset = %d, "
                      "key = %s, value = %s\n",
                      record.topic(), record.partition(), record.offset(),
                      deserialize(record.key()), deserialize(record.value()));
  }

  consumer.commitSync();
}
```

## Apache Pulsar

### Producer 

```cpp
Client client("pulsar://localhost:6650");
Producer producer;
Result result = client.createProducer("persistent://sample/standalone/ns1/my-topic", producer);
if (result != ResultOk) {
    LOG_ERROR("Error creating producer: " << result);
    return -1;
}
// Publish 10 messages to the topic
for(int i=0;i<10;i++){
    Message msg = MessageBuilder().setContent("my-message").build();
    Result res = producer.send(msg);
    LOG_INFO("Message sent: " << res);
}
client.close();
```

## Consumer


```cpp 
Client client("pulsar://localhost:6650");
Consumer consumer;
Result result = client.subscribe("persistent://sample/standalone/ns1/my-topic",
                                 "my-subscribtion-name", consumer);
if (result != ResultOk) {
  LOG_ERROR("Failed to subscribe: " << result);
  return -1;
}
Message msg;
while (true) {
  consumer.receive(msg);
  LOG_INFO("Received: " << msg << "  with payload '" << msg.getDataAsString()
                        << "'");
  consumer.acknowledge(msg);
}
client.close();
```

module System = {
  Storage,
  Partition,
  Broker,
  Producer,
  CommitProtocol,
  FetchProtocol
};

machine MultiPartitionProduceTest {
  start state Init {
    entry {
      var storage: Storage;
      var broker: Broker;
      var producer: Producer;
      var partitions: set[Partition];
      var num_partitions: int;
      var num_batches: int;

      num_partitions = 3;
      num_batches = 10;

      storage = new Storage();

      while (sizeof(partitions) < 3) {
        partitions += (new Partition());
      }

      broker = new Broker((storage = storage,));

      producer = new Producer((
        broker = broker,
        partitions = partitions,
        count = num_batches));
    }
  }
}

test tcMultiPartitionProduce [main=MultiPartitionProduceTest]:
  assert ProduceRequestResponse in
  (union System, { MultiPartitionProduceTest });

// This test case should not normally be run when checking the model. It is not
// expected to pass nor fail, but rather it is used as a tool for debugging and
// development. See `SelectStorageConfiguration` for more information.
test tcFindConfiguration [main=MultiPartitionProduceTest]:
  assert ProduceRequestResponse, SelectStorageConfiguration in
  (union System, { MultiPartitionProduceTest });

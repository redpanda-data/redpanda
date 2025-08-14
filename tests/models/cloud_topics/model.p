module System = {
  Storage,
  EpochService,
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
      var epoch_service: EpochService;
      var broker: Broker;
      var producer: Producer;
      var partitions: set[Partition];
      var num_partitions: int;
      var num_batches: int;

      num_partitions = 3;
      num_batches = 10;

      storage = new Storage();
      epoch_service = new EpochService();

      while (sizeof(partitions) < num_partitions) {
        partitions += (new Partition());
      }

      broker = new Broker((storage = storage, epoch_service = epoch_service));

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
//
// The way to do this is check the model with the tcFindConfiguration test case
// like this `p check -tc tcFindConfiguration -s 1000` and then if the test finds
// a failure (and the failure is this spec ending in the hot state) then the target
// configuration was found in some schedule.
test tcFindConfiguration [main=MultiPartitionProduceTest]:
  assert ProduceRequestResponse, SelectStorageConfiguration in
  (union System, { MultiPartitionProduceTest });

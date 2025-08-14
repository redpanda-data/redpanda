//
// A producer sends a configurable number of produce requests to randomly
// selected partitions. When a produce response is received the producer
// verifies that the expected batch can be fetched from the reported offset.
//

machine Producer {
  var broker: Broker;
  var partitions: set[Partition];
  var count: int;
  var request_id: int;
  var batch_id: int;

  // mapping from produce request id to produce request
  var produce_request: map[int, produce_request];

  // mapping from fetch request id to expected batch id
  var expected_fetch_batch: map[int, int];

  start state Init {
    entry (config: (broker: Broker, partitions: set[Partition], count: int)) {
      broker = config.broker;
      partitions = config.partitions;
      count = config.count;
      goto Produce;
    }
  }

  state Produce {
    entry {
      if (count > 0) {
        produce_request[request_id] = (
            source = this,
            request_id = request_id,
            partition = choose(partitions),
            batch_id = batch_id);

        send broker, produce_request_event, produce_request[request_id];

        count = count - 1;
        batch_id = batch_id + 1;
        request_id = request_id + 1;

        // Note the self-transition below used to drive generation of produce
        // requests does not introduce a scheduling point. This should not
        // reduce model exploration in a meaningful way because (1) the client
        // behavior is not affected by scheduling and (2) all of the downstream
        // state exploration is driven by the order in which items are dequeued
        // from the fifo queues populated by the producer.
        goto Produce;
      }
    }

    on produce_response_event do (response: produce_response) {
      var request: produce_request;

      // the original produce request
      request = produce_request[response.request_id];

      // the expected fetch batch will be identical to the batch in the original
      // produce request whose response contains the offset being fetched.
      expected_fetch_batch[request_id] = request.batch_id;

      send broker, fetch_request_event, (
        source = this,
        request_id = request_id,
        partition = request.partition,
        offset = response.offset);

      request_id = request_id + 1;
    }

    on fetch_response_event do (response: fetch_response) {
      assert response.batch_id == expected_fetch_batch[response.request_id];
    }
  }
}

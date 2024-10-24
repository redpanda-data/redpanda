//
// The broker handles requests like Produce and Fetch.
//

// produce_request - append a batch to a partition
//
// source: the machine sending the request
// request_id: must be unique per source.
// partition: target partition for the operation
// batch_id: the batch "content".
//
// behavior of the system is not affected by the content of a batch. but it is
// still useful to generate unique batch "content" for use in assertions.
type produce_request = (
  source: machine,
  request_id: int,
  partition: Partition,
  batch_id: int);

// produce_response - returns a produced batch's offset
//
// request_id: value from the request. used for correlation.
// offset: offset at which the batch in the request is located.
type produce_response = (
  request_id: int,
  offset: int);

// fetch_request - request a batch from a partition
//
// source: the machine sending the request
// request_id: correlation id. unique per source.
// partition: partition to read from
// offset: offset of the batch to read
type fetch_request = (
  source: machine,
  request_id: int,
  partition: Partition,
  offset: int);

// fetch_response - returns the fetched batch
//
// request_id: correlation id from request
// batch_id: the batch "content"
type fetch_response = (
  request_id: int,
  batch_id: int);

// Produce request and response events
event produce_request_event : produce_request;
event produce_response_event : produce_response;

// Fetch request and response events
event fetch_request_event : fetch_request;
event fetch_response_event : fetch_response;

// Internal event used by the broker to drive the commit protocol.
event broker_commit_event;

machine Broker {
  var storage: Storage;
  // received produce requests that will be committed together
  var produce_requests: seq[produce_request];
  var request_id: int;

  start state Init {
    entry (config: (storage: Storage)) {
      storage = config.storage;
      goto WaitForRequest;
    }
  }

  state WaitForRequest {
    on produce_request_event do (request: produce_request) {
      // produce requests are queued and processed later.
      produce_requests += (sizeof(produce_requests), request);

      // if the pending set just transitioned to the non-empty state then ensure
      // that it is eventually processed (there may be no more produce requests
      // to drive execution). scheduled processing for "later" captures the
      // batch size non-determinism of a real system either having reached a maximum
      // latency, or accumulated a maximum number of bytes.
      //
      // In a real-world system a batching policy will exist. That could be
      // "wait until 4mb have accumulated", or "wait at most 200ms", or "ask
      // ChatGPT when enough batches have been accumulated". The point is that
      // the batching policy should not affect correctness.
      //
      // When I talk about "capturing the non-determinism of a real system" what
      // I mean is that the model uses a scheduling point to drive the policy of
      // which produce requests are included in an L0d object, so the model
      // itself captures a wide range of possible policies.
      if (sizeof(produce_requests) == 1) {
        send this, broker_commit_event;
      }
    }

    on fetch_request_event do (request: fetch_request) {
      new FetchProtocol((storage = storage, partition = request.partition, request = request));
    }

    // Commit and reset the accumulated set of produce requests.
    on broker_commit_event do {
      new CommitProtocol((storage = storage, requests = produce_requests));
      produce_requests = default(seq[produce_request]);
    }
  }
}

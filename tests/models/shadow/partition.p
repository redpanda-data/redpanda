//
// The partition represents a replicated, append-only log. When an entry is
// appended to the log its offset is returned.
//

// placeholder_batch - reference to a batch in an L0d object
//
// L0d_object_id: identifier of the object in cloud storage
// L0d_offset: offset of batch within the object
type placeholder_batch = (
  L0d_object_id: int,
  L0d_offset: int);

// append_request - request to append a batch to the partition's log
//
// source: the machine sending the request
// request_id: correlation id
// batch: the batch to append
type append_request = (
  source: machine,
  request_id: int,
  batch: data);

// append_response - response to an append request
//
// request_id: use the value from the request.
// offset: the offset at which the batch was written
type append_response = (
  request_id: int,
  offset: int);

// read_request - request to read a batch from the partition's log
//
// source: the machine sending the request
// request_id: correlation id
// offset: the offset to read
type read_request = (
  source: machine,
  request_id: int,
  offset: int);

// read_response - response to a read request
//
// request_id: correlation id
// batch: the batch stored at the requested offset
type read_response = (
  request_id: int,
  batch: data);

// Partition append request and response events
event append_request_event : append_request;
event append_response_event : append_response;

// Partition read request and response events
event read_request_event : read_request;
event read_response_event : read_response;

machine Partition {
  var log: seq[data];

  start state WaitForRequest {
    on append_request_event do (request: append_request) {
      var offset: int;
      offset = sizeof(log);
      log += (offset, request.batch);

      send request.source, append_response_event, (
        request_id = request.request_id,
        offset = offset);
    }

    on read_request_event do (request: read_request) {
      send request.source, read_response_event, (
        request_id = request.request_id,
        batch = log[request.offset]);
    }
  }
}

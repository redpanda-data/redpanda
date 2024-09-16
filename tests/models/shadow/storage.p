//
// Represents cloud storage like S3. Unlike real-world cloud storage systems
// the model returns a unique id when writing a new object. This is fine in the
// current model because this merely replaces what would otherwise be UUID name
// generation.
//

// put_request: store data in cloud storage
//
// source: the machine from which the request originated
// request_id: must be unique per source.
// object: the data being stored in the object
type put_request = (
  source: machine,
  request_id: int,
  object: data);

// put_response: response to put_request
//
// request_id: value from the request. used for correlation.
// object_id: unique id for the stored object
type put_response = (
  request_id: int,
  object_id: int);

// get_request: read an object from storage
//
// source: machine from which the request originated
// request_id: correlation id
// object_id: the object identifier
type get_request = (
  source: machine,
  request_id: int,
  object_id: int);

// get_response: response to get_request
//
// request_id: correlation id from request
// object: the object data
type get_response = (
  request_id: int,
  object: data);

// Put request and response events
event put_request_event : put_request;
event put_response_event : put_response;

// Get request and response events
event get_request_event : get_request;
event get_response_event : get_response;

// Internal event for monitoring
event monitor_storage_event: (object_id: int, object: data);

machine Storage {
  var next_object_id: int;
  var objects: map[int, data];

  start state HandleRequest {
    on put_request_event do (request: put_request) {
      objects += (next_object_id, request.object);
      send request.source, put_response_event, (
        request_id = request.request_id,
        object_id = next_object_id);

      announce monitor_storage_event, (
        object_id = next_object_id,
        object = request.object);

      next_object_id = next_object_id + 1;
    }

    on get_request_event do (request: get_request) {
      send request.source, get_response_event, (
        request_id = request.request_id,
        object = objects[request.object_id]);
    }
  }
}

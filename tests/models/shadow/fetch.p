//
// The Fetch Protocol handles a fetch request received by a broker. It
// coordinates access to the partition, cloud storage, and responds to the
// initiating fetch request.
//

machine FetchProtocol {
  var storage: Storage;
  var partition: Partition;
  var request: fetch_request;

  start state Init {
    entry (input: (storage: Storage, partition: Partition, request: fetch_request)) {
      storage = input.storage;
      partition = input.partition;
      request = input.request;
      fetch();
    }
  }

  fun fetch() {
    var placeholder: placeholder_batch;
    var object: L0d_object;
    var request_id: int;

    // A fetch request is handled by first reading the placeholder batch out of
    // the underlying partition. Its offset is contained in the fetch request.
    send partition, read_request_event, (
      source = this,
      request_id = request_id,
      offset = request.offset);

    receive {
      case read_response_event: (response: read_response) {
        assert response.request_id == request_id;
        placeholder = response.batch as placeholder_batch;
      }
    }

    // After the placeholder is read from the partition the next step is to read
    // the referenced L0d object from cloud storage.
    request_id = request_id + 1;
    send storage, get_request_event, (
      source = this,
      request_id = request_id,
      object_id = placeholder.L0d_object_id);

    receive {
      case get_response_event: (response: get_response) {
        assert response.request_id == request_id;
        object = response.object as L0d_object;
      }
    }

    // Finally the target batch is plucked out of the L0d object and a response
    // to the original fetch request is sent back to the client.
    send request.source, fetch_response_event, (
      request_id = request.request_id,
      batch_id = object[placeholder.L0d_offset]);
  }
}

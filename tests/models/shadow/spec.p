// A liveness specification that checks that a response is sent for every
// produce request received by a broker.
spec ProduceRequestResponse observes produce_request_event, produce_response_event {
  var requests: set[int];

  start state Init {
    on produce_request_event do handle_request;
  }

  hot state AwaitingResponses {
    on produce_response_event do (response: produce_response) {
      assert response.request_id in requests;
      requests -= (response.request_id);
      if (sizeof(requests) == 0) {
        goto Init;
      }
    }

    on produce_request_event do handle_request;
  }

  fun handle_request(request: produce_request) {
    assert !(request.request_id in requests);
    requests += (request.request_id);
    goto AwaitingResponses;
  }
}

// This monitor is expected to end in a hot state, by design. It's intended to
// be used not to check a correctness property, but rather as a tool for finding
// execution schedules of interest, which I've found to be useful in debugging
// and development. To use this monitor, describe the configuration of interest
// by modifying the `target` map below (see "set-target-configuration"). Note
// that in general the target configuration needs to be co-designed with the
// main test harness. For example, 2 produced batches, 1 object with 2 batches.
spec SelectStorageConfiguration observes monitor_storage_event {
  var objects: map[int, L0d_object];

  start state Init {
    on monitor_storage_event do handle_event;
  }

  hot state TargetConfig {
    on monitor_storage_event do handle_event;
  }

  fun handle_event(e: (object_id: int, object: data)) {
    objects += (e.object_id, e.object as L0d_object);
    if (target_config()) {
      goto TargetConfig;
    }
    goto Init;
  }

  fun target_config(): bool {
    var size: int;
    var object: L0d_object;
    var target: map[int, int];
    var sizes: map[int, int];

    foreach(object in values(objects)) {
      size = sizeof(object);
      if (size in keys(sizes)) {
        sizes[size] = sizes[size] + 1;
      } else {
        sizes[size] = 1;
      }
    }

    // set-target-configuration
    //
    // example: find an execution schedule with 2 objects containing 4 batches
    // each, and 2 objects containing 1 batch each, for a total of 10 batches.
    target[4] = 2;
    target[1] = 2;

    return sizes == target;
  }
}

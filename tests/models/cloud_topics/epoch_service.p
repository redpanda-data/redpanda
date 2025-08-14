// Singleton cluster service

// get_epoch_request: retrieve the current epoch value
//
// source: machine from which the request originated
// request_id: correlation id
type get_epoch_request = (
  source: machine,
  request_id: int);

// get_epoch_response: response to get_epoch_request
//
// request_id: correlation id
// epoch: the current epoch value
type get_epoch_response = (
  request_id: int,
  epoch: int);

// get request and response events
event get_epoch_request_event : get_epoch_request;
event get_epoch_response_event : get_epoch_response;

// Internal event used to increment the epoch
event epoch_increment_event;


// ideally we could model epoch increment like this
//
//   on epoch_increment_event {
//     epoch = epoch + 1;
//     send this, epoch_increment_event;
//   }
//
// because in practice the epoch source may run at any speed. however, allowing
// this transition to always be enabled slows the model down by orders of
// magnitude.
//
// there are various tricks we can play. the approach taken here is to simulate
// background epoch increment but only trigger the simulation when the resource
// is examined. that is, reading the epoch will trigger an asynchronous
// increment of the epoch between [0, N). From the perspective of the next
// read, the epoch may or may not have advanced.
machine EpochService {
  var epoch: int;

  start state Init {
    entry {
      epoch = 0;
      goto WaitAndIncrement;
    }
  }

  state WaitAndIncrement {
    entry {
      epoch = epoch + choose(100);
    }

    on get_epoch_request_event do (request: get_epoch_request) {
      send request.source, get_epoch_response_event, (
        request_id = request.request_id,
        epoch = epoch);
      send this, epoch_increment_event;
    }

    on epoch_increment_event goto WaitAndIncrement;
  }

}

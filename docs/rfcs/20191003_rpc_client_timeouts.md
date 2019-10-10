- Feature Name: Timeouts in RPC client
- Status: draft
- Start Date: 2019-10-03
- Authors: michal
- Issue: #137

# Summary

The RPC stack is asynchronous (i.e. clients do not block while waiting for the
response). The RPC client implementation uses the correlation map
to identify and dispatch responses for particular requests.
The RPC client returns the `seastar::future<..>` that will fire up continuations
whenever the response is ready. Currently if the response is not delivered
to the client, the returned future will never be completed. In order to prevent
this situations the timeouts have to be introduced into RPC client stack.
The user should be able to pass the timeout as one of the method arguments,
so that each request can have different timeout. When the time of
processing client request is longer then requested timeout the client should
return `make_exception_future<>()` with designated type of exception.

# Motivation

Timeouts are required in RPC stack as waiting for the response
from remote server can last indefinitely.

# Guide-level explanation

## Client API

The timeout parameter will be added as a last optional argument to generated,
client methods. By default there will be no timeout.

```c++
future<ret_t> method_1(agr1_t arg1, rpc::clock_type::time_point timeout)
```

where:

```c++
static constexpr rpc_clock::time_point no_timeout = rpc_clock::time_point::max();
  ```

In order to use an absolute duration as a timeout (f.e. 5 seconds) the caller
have to add `rpc::duration_type` to `rpc::clock_type::now()` every time the method is
called in order to make it timeout after requested amount of time.

Every time the timeout value passed to the method is different than `rpc::no_timeout`
the `rpc::client` logic will arm a `seastar::timer`, before sending
an actual request to server, to fire at requested time point.

When the timer fires it will fail the outstanding `future` related with
particular request so that the `rpc::timeout_exception` will be propagated
to the caller.

## Detailed design

Right now the outstanding requests responses are kept in
`std::unordered_map<uint32_t, promise_t>` where
`promise_t = promise<std::unique_ptr<streaming_context>>`.
The `promise_t` is used to signal the client that the response is ready.
If the timeout has elapsed the promise must be completed with an exception
and removed from the correlation map, additionally all the processing related
with this request should be aborted. In order to do this each request have to
have a timer related with it. The solution is to keep wrap the promise_t with an
additional structure that will keep the timeout timer, only in case when the
timeout parameter was provided. The structure will look like this:

```c++

CONCEPT(
    template<typename R> concept ResponseHandler = requires(R r, std::exception& e) {
    { r.complete_exceptionally(e) } -> void;
    { r.complete(e) } -> void;
    { r.get_future()} -> future<std::unique_ptr<streaming_context>>;
};)

template<typename Impl>
CONCEPT(requires ResponseHandler<Impl>)
struct response_handler {
    template<typename... Args>
    response_handler(Args... args) {
        std::make_unique<Impl>(std::forward<Args>(args)...);
    }

    using response_ptr = std::unique_ptr<streaming_context>;

    future<response_ptr>  get_future() {
        return _impl->get_future();
    }

    void complete_exceptionally(std::exception& e) {
        _impl->complete_exceptionally(e);
    }

    void complete(response_ptr r) {
        _impl->complete(std::move(r));
    }

private:
    std::unique_ptr<Impl> _impl;
};
```

The `Impl` without timeout will only contain a `promise_t` instance whereas
the timeout version will contain a timeout timer of type `rpc::timer_type`.
The timer will be armed during construction of the `timeout_response_handler`
and it will execute the following action (passed as lambda to constructor):

```c++
    auto it = _correlations.find(correlation_id);
    auto response_handler = std::move(it->second);
    _correlations.erase(it);
    response_handler.complete_exceptionally(std::move(ctx));
```

Additionally the `rcp::client::dispatch` method can not fail when the response
with some correlation id was not found in the map.

The timer will be deactivated after when
either `complete()` or `complete_exceptionally()` method will be called.

## Drawbacks

Increased memory usage especially in cases where there are a lot of outstanding
RPC requests.

## Rationale and Alternatives

- Why is this design the best in the space of possible designs?

The design is based of a battle tested Seastar internals.
The Seastar timers implementation is lightweight and it has tunable accuracy.
In the proposed solution the single timeout accounts for all the parts of RPC stack,
serializing and sending request, waiting for response,
therefore the design and implementation is simple and straight forward.

- What other designs have been considered and what is the rationale for not
  choosing them?

Implementing the timeouts based on the polling,
iterating over futures waiting for response every certain amount of time
and failing these futures thats timeout elapsed. The solution would be however
more complicated.

## Unresolved questions

- How to differentiate the situation when server will respond with corrupted
  correlation id from the one when timeout occurred.

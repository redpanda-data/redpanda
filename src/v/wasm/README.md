# WebAssembly Engine Library

This library contains the bulk of the functionality related to WebAssembly Virtual Machines and their integration into Redpanda.
Currently, this library's focus is in support of Data Transforms, so the APIs are focused around that. If additional functionality
for Wasm was introduced to Redpanda in the future, the `wasm::engine` abstraction would need to split, as there would be an engine
in support of each plugin functionality needed.

The major abstractions of this package are listed below. Note that it's important that these abstractions are solid - we take advantage
of the abstractions in numerous places to provide functionality like caching/reuse of engines on a core, dummy engines for testing or even
engines that exist on different computers.

#### **Runtime**

The main entry point into this library, it holds the configuration for the entire subsystem and has the ability to compile a Wasm binary
into a _Factory_. There is only a single logical runtime for a process and access to that runtime is normally coordinated on shard 0.

#### **Factory**

A factory is a tricky object. A factory represents a compiled artifact that can cheaply be made into engines for a given core. Since 
compilation and the resulting artifacts are very expensive, we only create one per process and share them around cores (use 
`seastar::foreign_ptr`). But, whichever core makes an engine using this factory, the engine must only be used on that core.
This can be a little mind boggling to work through, but thankfully most users of this API can just ask for an engine on their 
core, and this logic can be a single isolated function.

#### **Engine**

Engine is the workhorse of the data plane for Data Transforms. It's the part that actually takes batches and writes them into the Wasm
runtime then reads the results back out. In principle it's quite simple, but in reality it's a bit of a dance to do this in a high 
performance manner. There are various modules that are stored within an engine and exposed to the guest code running inside the VM.
See the FFI section for more information on how to extend and add new functionality.

## Wasmtime

Our WebAssembly VM is built using [Wasmtime][wasmtime]'s [C-API][c-api]. Wasmtime itself is a very high performance Wasm VM, but is still
lightweight and is customizable in all the ways we require. For example, by default Wasmtime `mmap`'s memory for stacks and linear memory
inside of the VM at runtime, but also allows plugging in custom allocators (which we use as Seastar has strict opinions about memory). To learn
more about the allocators see `allocator.h`.

We use Wasmtime's ability to run the guest code on another stack extensively. In wasmtime's documentation you'll see this referenced as "async" APIs.
Wasmtime will execute the guest using an explicit stack, and switch back to the original caller's stack to support suspending the VM. This is how host
functions can return futures. We tell the VM to switch back and then once we've switched back we tell Seastar's scheduler to re-schedule running the VM
after the host function's future has resolved - the details here are in `invoke_async_host_fn` and `wasmtime_engine::_pending_host_function`.

#### Caveats

Wasmtime uses Unix signals extensively to implement specific behaviors (ie aborts in the VM), which has in the past caused issues with Seastar. 
Seastar by default blocks (most) signals for all shards except shard0, and has different behaviors in debug mode when ASAN is enabled. To combat this
we unblock all signals that Wasmtime needs on startup for all cores when Wasm is enabled. If Wasmtime changes the signals they use we will want to
update our list of signals we force Seastar to unblock. Lastly, there are some complications with seastar::thread and it usage of `swapcontext` that
can preserve signals and reblock them at times. Thankfully since we don't use seastar::thread much in production this isn't an issue in practice.

## Foreign Function Interface (FFI)

WebAssembly on it's own is completely isolated/sandboxed and has no ability to interact with the host or outside. We have a small custom framework
we've written to make it easy (-ish) to expose C++ classes into the VM. The general conversion code between C++ methods and Wasm memory is in `ffi.h`,
but the actual registration code and hooking that up is in `wasmtime.cc` - in particular the `host_function::reg` function, which is generally wrapped
in a `REG_HOST_FN` macro for the modules we support.

### Current Modules

We currently have the following list of modules exposed to guest code running inside the VM. How one would call these modules from a given language
can be a little mysterious, but we currently (at time of writing) have examples checked into the tree for C/C++, Golang and Rust.

#### transform_module

The transform module is the heart of transforming models and has a tight integration with the `wasmtime_engine` class. This is because we always have
the engine "active" so to speak. All of our SDKs at their core is a loop like the following:

```python
def main():
    check_abi()
    while True:
        n_records = read_batch_header()
        for _ in range(n_records):
            r = read_record()
            for out in transform(r):
                write_record(r)
```

When there are no batches left `read_batch_header` suspends the VM until another record comes along. Most of the complexity of the VM lifetime is due to this
approach where the VM polls the host for records (as opposed to the broker calling into the VM for each record to transform). On similar note - the reason we
use a per record interface to the VM instead of a per batch interface is for a couple of reasons. One, we can easily set limits like timeouts per record and they
become more intuitive for our users (who think per record). Two, record sizes are generally less variable than batch sizes, so it becomes easier to size functions
for memory requirements.

#### schema_registry_module

In order to access schema registry, we expose a custom module for using a few key schema registry APIs. We wrap the schema registry subsystem in a custom
class because it's easier to use and we also are able to swap out that class for testing. See `schema_registry.h` for the details.

#### wasi_module

WASI is a standard for WebAssembly interacting with POSIX-like host APIs such as filesystems, clocks, environment variables, etc. We provide a custom
implementation of these APIs because most upstream toolchains/languages assume at least a POSIX environment, and it's a more familar programming
experience for our users. We just stub out most of the APIs, but some of them (like environment variable support) is fully there.

[wasmtime]: wasmtime.dev
[c-api]: https://docs.wasmtime.dev/c-api/

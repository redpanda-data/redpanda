# WebAssembly Parser Library

The Wasm parser library contains routines to parse WebAssembly in the binary format. This can be used to do static analysis of WebAssembly modules as they are deployed.
We only support what is defined in the WebAssembly v2 Core Spec. We do not support any proposals that have not been merged into the spec. We may choose to add some
support for extra functionality as we make use of it in the Wasm subsystem. Lastly, this is not a full parser, but only parses the "API" so to speak of a module. Specifically
that is imports and exports of the defined types and excludes parsing instructions (function bodies).

If you're looking to develop this library or make changes the following links maybe helpful to understand WebAssembly's binary format:

* [Offical WebAssembly Core Specification](https://webassembly.github.io/spec/core/)
* [Offical WebAssembly Testsuite](https://github.com/WebAssembly/testsuite)
* [WebAssembly Proposals](https://github.com/WebAssembly/proposals)
* [Understanding Every Byte in a WASM Module](https://danielmangum.com/posts/every-byte-wasm-module/)

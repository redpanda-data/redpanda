# coprocessors

The coprocessor implementation allows you to deploy functional units that 
operate on the data moving through redpanda logs. The advantage of using the 
built in coprocessor engine is a drastic improvement in performance, reducing 
the network chatter that would otherwise exist if you were to perform 
transformations off cluster, and it removes any operational complexity that 
would be introduced by needing to host/manage your transforms on seperate
hardware.

For now you can only deploy Javascript functions, however in the future we plan 
to support all of the WASM supported languages.

## Getting started

To start you'll need to ensure you have `nodejs`, `rpk` and `redpanda` installed 
on your machine. Then in your `redpanda.yaml` configuration file, you'll need to 
enable developer and coprocessing mode like so:

```
redpanda:
  ...
  developer_mode: true
  enable_coproc: true
  ...
```

### Creating your first coprocessor

To start, run `rpk wasm generate` in a fresh directory. This will write an 
example project scaffolding that you can use to get started with.

```
.
└── wasm
    ├── package.json
    ├── src
    │   └── wasm.js
    ├── test
    │   └── wasm.test.js
    └── webpack.js
```

Your source code will be within the `src` directory. Below is an explanation of 
the API but for now, there is a default operator in there that will consume 
from one topic called `produce` and generate messages this onto another topic 
called `result` while applying a transformation that just uppercases all 
characters detected on stream.

Once your done modifying the source, cd into the `wasm` directory and run `npm 
install`. You'll need to do this when you modify your package dependencies. 
Then run `npm run build` to package up the finished product. You'll see this 
in the `dist/` folder, with the same name as the source file:

```
wasm/dist
└── wasm.js
```

### Building & launching the WASM engine

To build the wasm engine:
```
$ vtools install js-deps && vtools build js
```

If your using the oss build you can run the `build.sh` script in `$root/src/js`.
The products will be placed in `$root/<vbuild or build>/node/output`. 

To start the server, cd into the `output` directory and run:
```
$ npm run start <path to redpanda.yaml>
```

Here's a list of currently supported options that the wasm engine will look for 
within your `redpanda.yaml` config file:

```
redpanda:
  coproc_supervisor_server: 43189
  ...
  
coproc_engine:
  path: "/home/workspace/coprocessors"

...
```

You should see three directories be created in your `coproc_engine::path`, 
`active` `submit` and `inactive`. If you didn't explicitly specify a path in 
your `redpanda.yaml` file, these folders will exist in their default location: 
`/var/lib/redpanda/coprocessor`

### Deploying the coprocessor

Currently the coprocessor implementation does not support registering 
coprocessors that desire to transform a topic that does not yet exist. So 
you'll have to first create topics for all of the topics you wish to transform. 
For working with our example that is just the topic named `produce`. For the 
following examples `rpk` is shown when creating, publishing and consuming from 
topics, but you can use any kafka api compatible client.

```
$ rpk api topic create -r 1 produce`
```

Finally your ready to deploy your coprocessor. Copy the coprocessor from the 
`wasm/dist/` folder in your project, into the `submit` directory within the 
wasm engines working path (let's call this the `$wasm_root`). You may need to 
use sudo depending on where the `$wasm_root` is.

```
$ cp wasm/dist/wasm.js $wasm_root/submit
```

The wasm engine has watchers in this directory and will automatically attempt 
to register these coprocessors with redpanda. The logs will show this:

```
2021-01-13T20:44:14.758Z [FileManager] info: Detected new file in submit dir: 
/home/robert/workspace/coprocessors/submit/wasm0.js
2021-01-13T20:44:14.779Z [FileManager] info: Initiating RPC call to redpandas 
coprocessor service at endpoint - enable_coprocessors with data: [object Object]
2021-01-13T20:44:14.787Z [FileManager] info: Succeeded in establishing a 
connection to redpanda
```

The coprocessors will start ingesting from topic offset 0, so if there is data 
you should begin to see redpanda making materialized logs, the relevent log 
to look for should look something like this:

```
INFO  2021-01-13 15:44:14,791 [shard 0] coproc - service.cc:104 - Request 
recieved to register coprocessor with id: 950570717575177792
INFO  2021-01-13 15:44:45,922 [shard 1] coproc - script_context.cc:298 - 
Making new log: {kafka/produce.$result0$/0}
```

If you don't already have data in this topic, you can use `rpk` to send some 
data over the test topic:

```
$ rpk api produce <source_topic>
```

### Test everything worked

To test everything worked you can use `rpk api consume` to read from the 
materialized topic. Note that materialized topics are prefixed with their 
source topic that the apply function had used when making the transform. And 
the destination portion of the topic is surrounded by dollar signs. This is not 
a valid kafka topic so you will not be able to write to this topic with any 
kafka producer.

```
# (Note the use of single quotes)
$ rpk api consume 'produce.$result$'
```

### Disabling coprocessors

If anything didn't work as expected the wasm engine will move scripts from the 
`submit` or `active` folder, into the `inactive` folder. However if you wish to 
stop processing on any given coprocessor, a manual `mv` of the coprocessor into 
the `$wasm_root/inactive` directory will trigger its proper shutdown and cause 
redpanda to no longer send data from the interested topics to the wasm engine.

```
$ mv $wasm_root/active/wasm.js $wasm_root/inactive
```

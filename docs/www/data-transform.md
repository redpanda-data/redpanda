---
title: Data transformation
order: 0
---
# Data transformation

**Important** - This feature is in technical preview; a cluster with this feature enabled is not yet supported for production.

Redpanda Data Transforms enable users to create WebAssembly-based scripts to perform simple data transformations on topics. 

This eliminates "data ping-pong" by removing the need to consume data streams from a separate system for simple transformations such as scrubbing or cleaning data.

With Redpanda, you can now deploy custom Node.js programs to handle the entire consume-transform-produce cycle for you.

We do all of this co-processing using a new transformation engine built on-top of [Wasm (WebAssembly)](https://developer.mozilla.org/en-US/docs/WebAssembly) technology.

All you need to do is to provide:
- A JavaScript code that transforms the data. 
- The source topics where to consume from.
  
The destination topics can be generated on demand when your transformation runs. 

You can jump start your data transformation development with `rpk`. We provide a sample Node.js project so you can start developing right away. Then you can deploy the data transform to Redpanda for all of the brokers to run.

Be aware that even though we're referring to this feature as data transformation, the Kafka topics are still immutable. 
What happens under the hood, is that we're reading from an immutable Kafka topic, applying a data transformation process and then we producing to another immutable Kafka topic.
Your data is never actually transformed, but rather read and written again. 

## Enable data transforms

To enable the coprocessor that runs the data transformation you need to enable the engine in the Redpanda configuration file. 

In your `redpanda.yaml` configuration file (which the default path is `/etc/redpanda/redpanda.yaml`) add these **required** flags under `redpanda`.

```yaml
enable_coproc: true
developer_mode: true
enable_idempotence : false 
```

You can change the port in which the coproc reads from, this is entirely optional. To achieve this add this section:

```yaml
coproc_supervisor_server:
    address: 0.0.0.0
    port: <new port>
```

The default location of the data transforms log file is: `/var/lib/redpanda/coprocessor/wasm_engine.log`. You can configure the location of this file in `redpanda.yaml` like this: 

```yaml
coproc_engine:
  logFilePath: <file_path>
```

If you opt-in for all the additional configs your `redpanda.yaml` should look similar to this: 

```yaml
config_file: /etc/redpanda/redpanda.yaml
node_uuid: mecCncdqAUKFoM2ojvR2Ai86kGWuhiJ95xpNCsKwe6VTgvHoJ
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: true        ## since this is a tech preview you must set developer mode as true
  enable_coproc: true         ## data transform flag here. make sure it's under redpanda
  enable_idempotence : false  ## this flag is also mandatory because we're running a tech preview
  coproc_engine:
  logFilePath: <file_path>    ## remember to change the file_path
  coproc_supervisor_server:
    address: 0.0.0.0          ## if you want, you can change the ip address here
    port: <new port>          ## remember to change the port here
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers: []
rpk:
  coredump_dir: /var/lib/redpanda/coredump
  enable_memory_locking: false
  enable_usage_stats: false
  overprovisioned: false
  tune_aio_events: false
  tune_ballast_file: false
  tune_clocksource: false
  tune_coredump: false
  tune_cpu: false
  tune_disk_irq: false
  tune_disk_nomerges: false
  tune_disk_scheduler: false
  tune_disk_write_cache: false
  tune_fstrim: false
  tune_network: false
  tune_swappiness: false
  tune_transparent_hugepages: false
schema_registry: {}
```

Save your changes.

Now that you have the configuration setup let's start the Wasm engine itself.

**Important**. You can only activate the engine when Redpanda is not running. If Redpanda is running you can stop it with this command:

```bash
sudo systemctl stop redpanda
```

To start the Wasm engine you can run this command:

```bash
sudo systemctl start wasm_engine
```

If you're running it as a service, you can either start it by running this command: 

```bash
sudo systemctl start redpanda
```

or to restart it:

```bash
sudo systemctl restart redpanda
```

## Generating the data transform package

The data transform is packaged in a Node.js project and uses the Wasm (WebAssembly) instruction format.

Make sure Node.js version 12 or higher is installed on the machine where you run prepare the transform bundle.

To create the template project, run: 

```bash
rpk wasm generate <project_name>
```

Remember to change the `project name`.

For example, let's create a project that will change the text in your events to all uppercase:

```bash
rpk wasm generate uppercase
```

The project is created in a directory named for the project and including these files:

```bash
**uppercase/**
├── package.json
├── **src**
│   └── main.js
├── **test**
│   └── main.test.js
└── **webpack.js**
```

The most important files to know about in the project are:

- /src/main.js - This file contains your transform logic and hooks into the API to define the event inputs.
- /src/package.json - If your transform requires Node.js dependencies you need to add them to this file.

> **_Note_** - If you want to seem what else `rpk wasm` can do, run the help command with:
```bash
rpk wasm -h
```

## The sample project

The sample project contains this `main.js` file:
```js
const {
  SimpleTransform,
  PolicyError,
  PolicyInjection
} = require("@vectorizedio/wasm-api");
const transform = new SimpleTransform();
/* Topics that fire the transform function */
transform.subscribe([["test-topic", PolicyInjection.Stored]]);
/* The strategy the transform engine will use when handling errors */
transform.errorHandler(PolicyError.SkipOnFailure);
/* Auxiliar transform function for records */
const uppercase = (record) => {
  const newRecord = {
    ...record,
    value: record.value.map((char) => {
      if (char >= 97 && char <= 122) {
        return char - 32;
      } else {
        return char;
      }
    }),
  };
  return newRecord;
}
/* Transform function */
transform.processRecord((recordBatch) => {
  const result = new Map();
  const transformedRecord = recordBatch.map(({ header, records }) => {
    return {
      header,
      records: records.map(uppercase),
    };
  });
  result.set("result-topic", transformedRecord);
  // processRecord function returns a Promise
  return Promise.resolve(result);
});
exports["default"] = transform;
```

Let's dissect this file and understand what every line is doing here.

First we need to import these const from the Wasm API.

```js
const {
  SimpleTransform,
  PolicyError,
  PolicyInjection
} = require("@vectorizedio/wasm-api");
```

We then create a constant variable to hold the function SimpleTransform. This is the main function that we're going to use in the project.

```js
const transform = new SimpleTransform();
```

We fill the subscribe list with our topic, and the policy in which we're going to process new messages.

```js
transform.subscribe([["test-topic", PolicyInjection.Stored]]);
```

To add multiple source topics, add the topic and policy as pairs:

```js
transform.subscribe[[<topic1>,<policyA>],[<topic2>,<policyB>]]
```

> **_Note_** - If the topic does not exist, you can face a deployment error. It's recommended to create all of your source topics before you deploy your transformation.
> You can do that by running `rpk topic create test-topic`

The possible `PolicyInjection` values are:

- `PolicyInjection.Earliest` - The earliest offset. Transform all of the events in the topic from offset 0.
- `PolicyInjection.Latest` - The latest offset. Transform only the current incoming events.
- `PolicyInjection.Stored` - The stored offset. Transform the events starting from the latest recorded offset on disk. If no offsets are recorded then the earliest offset is processed.

Then we set the policy telling the co-proc how to handle errors: 

```js
transform.errorHandler(PolicyError.SkipOnFailure);
```

The possible `PolicyError` values are:

- `PolicyError.SkipOnFailure` - If there's a failure, skip to the next event.
- `PolicyError.Deregister` - If there's a failure, the coproc will be removed. 

Here we find the logic behind applying the uppercase. There's several ways to uppercase a word, but here we're flipping the ASCII table to uppercase every alphabetical char.

```js
/* Auxiliar transform function for records */
const uppercase = (record) => {
  const newRecord = {
    ...record,
    value: record.value.map((char) => {
      if (char >= 97 && char <= 122) {
        return char - 32;
      } else {
        return char;
      }
    }),
  };
  return newRecord;
}
```

We apply the created logic into the processRecord function. The `transformedRecord` variable will obtain a `recordBatch` from the topic that we subscribed to, apply the `uppercase` function and store a map called `records`. 

The generated `transformedRecord` will be set into the topic named `result-topic`. 

To finish we create a promise required by the API to process and we end by exporting this transform.

```js
/* Transform function */
transform.processRecord((recordBatch) => {
  const result = new Map();
  const transformedRecord = recordBatch.map(({ header, records }) => {
    return {
      header,
      records: records.map(uppercase),
    };
  });
  result.set("result-topic", transformedRecord);
  // processRecord function returns a Promise
  return Promise.resolve(result);
});
exports["default"] = transform;
```

You can change the `result-topic` to any topic name that you like. 

To produce onto more than one destination topic, add another line in the format:
```js
result.set("<destination_topic>", transformedRecord);
```

> **_Note_**: The actual name of the destination topic is created with the name format of: `<source>._<destination>_`. If the destination topic does not previously exist, its created automatically during script deployment.

Beware that if you have other mechanisms to auto-generate topics, such as having `auto_create_topics_enabled` set to `True` in your Redpanda configuration file, you might run into issues. In this example, if you setup a consumer before your transformation starts to write data into it, Redpanda will create a topic automatically for the consumer and the coproc won't be able to write data into it. 

The batch API is Bytes-In-Bytes-Out. We highly recommend that you build deterministic functions based on the input to make it easy to debug your applications.

If your transform requires Node.js dependencies, add them to the `/src/package.json` file.

## Prepare the script for deployment

Because the transform is packaged in a Node.js project, you need to install the dependencies and build the script that runs the transform.

Remember, you need Node.js version 12 or higher.

To do this, run these commands in the project directory:

```bash
npm install
npm run build
```

The build command creates the `main.js` JavaScript file in the `/dist` directory that contains the compiled transform bundle.

## Deploy the transform

To get the transform to start consuming and producing events, you need to deploy it in Redpanda with a name and description.

As with other `rpk` commands, you must specify the brokers in the cluster and all of the authentication parameters
(including user, password, TLS) for the brokers.

> **_Note_**: If the source topic does not exist the deployment will fail. If the target topic already exists, it will use the existing topic. 

To deploy the sample transform, run:

`rpk wasm deploy uppercase/dist/main.js --name uppercase --description "Converts uppercase text to lowercase"`

## Verify that the transform works

After the transform is deployed, Redpanda processes every event produced to the source topic
and runs the logic that is defined by the transform.

To see our sample transform in action:

1. Run: `rpk topic consume test_topic._result_`
2. Produce events to the source topic:
    a. In a second terminal run: `rpk topic produce test-topic`
    b. Enter text and press CTRL+D to send the event to the source topic.
3. In the terminal that shows the consumed events you’ll see the text that you produced but it is now formatted with uppercase characters.

## Clean up the transform

To stop a transform, you have to remove the transform from the cluster brokers.

For our sample transform, run: `rpk wasm remove uppercase`

The transform stops processing events and is removed.

## Deleting output topics 

Deleting output topics works just like deleting a normal Kafka topic. 

We recommend that you shutdown your coprocessors that are producing to this topic before you execute the deletion command. 

You can delete a topic with any Kafka client, or you can use `rpk` and execute this command:

```bash
rpk topic delete |topic|
```

## Source code

You can check what are all the possible export values in our [GitHub Page](https://github.com/vectorizedio/redpanda/blob/dev/src/js/modules/public/Coprocessor.ts).

The [API package](https://www.npmjs.com/package/@vectorizedio/wasm-api) is published under `npm`.

## Final considerations

Currently we only support Node.js but in the future, we're planning to add support for other supported Wasm languages. 

### Running untrusted code

We do not recommend you to run untrusted code. The reason for this is that the service will be running with the same privileges as Redpanda and with permission to read your data. 

The golden rule here is to only run code where you trust the authors or that you wrote yourself.

### Under the hood

We create a global control topic for each Redpanda cluster with the name `coproceessor_internal_topic`. This is done to support any Kafka client so you don´t *need* to use `rpk`. If anyone publishes to this topic, it will invoke a deployer implicitly. You just need to publish with the same headers and the correct format that we use. 

### Scaling

If you execute different types of transformations in different topics it's more advantageous to have multiple deployments. Remember that every time a transformation happens, you're subscribing to a topic and producing to another. Even though this process is really fast because it happens inside Redpanda, there's still a processing overhead. 

### Tech Preview Caveats

We don't use [Raft](https://raft.github.io/) to publish and replicate the data. The reason is because the same transform is applied locally on all nodes in the cluster. So it's possible to have inconsistencies in some cases. For example, if you have a topic with a replication factor > 1 and you have stateful or idempotent coprocessors producing different data onto these replication topics. 

The coprocessor currently doesn't have any system limits. For example there's not a limit in processing time, memory usage or number of topics created.

The coprocessor doesn't save your state. In the case of a crash, for example, if you have a counter, you'll lose that counter. The best practice here is to always avoid stateful implementations. The only state that it's kept and checkpointed is your offset, just like Kafka. The semantics of your materialized topics is `At-Least-Once`. Redpanda saves the offset from wherever you were reading. In the case of a crash, your deployed code will be reprocessed, based on your `PolicyInjection` policy. If you use `PolicyInjection.Stored` for example, then it will be reprocessed from whatever offset was saved before the crash.

It's not possible to pipeline multiple scripts so that you can pass transforms through one another (such as A->B->C->D). The API only maps topic(s)-to-topic(s). 

We would love to hear your feedback about how you want to use this feature. You can check what are the latest news, releases and talk to other members of the community by joining our [Slack community](https://vectorized.io/slack). Feel free to send us a message. 
We're happy to help you succeed.
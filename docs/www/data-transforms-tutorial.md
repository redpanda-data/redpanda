---
title: Data transforms tutorial
order: 0
---
# Data transforms tutorial

The basic process of streaming data is to have systems produce events to a topic
and have other systems consume from that topic.
What if you need to change the content of the events, or transform the events, 
in some way like scrubbing private data?
Normally you'd have to set up another process to consume the events, transform them,
and produce them to another topic that your consumers can consume from.
That's extra overhead of code, integration, monitoring, and performance load that you'd like to avoid if possible.

In typical Redpanda fashion, data transforms can do the consume-transform-produce cycle all within Redpanda.
All you need to do is provide a JavaScript transform that changes the data, the source topics to consume from,
and the destination topics to produce the resulting events to.

You can set the transform to process events from:

- The earliest offset - Transform all of the events in the topic from offset 0.
- The latest offset - Transform only the current incoming events.
- The stored offset - Transform the events starting from the latest recorded offset on disk. If no offsets are recorded then the earliest offset is processed.

We provide you with a Node.js project that Redpanda processes and you add your JavaScript to that project.
Then you deploy the data transform to the Redpanda for all of the brokers to run.

THIS FEATURE IS A TECHNICAL PREVIEW - You can give us feedback to help improve it
in [our Slack community](https://join.slack.com/t/vectorizedcommunity/shared_invite/zt-ng2ze1uv-l5VMWSGQHB9gp47~kNnYGA).

## Enable data transforms in the redpanda configuration file

Before you start creating data transforms, you need to enable the data transform engine in the redpanda configuration.

Edit the `/etc/redpanda/redpanda.yaml` file and make sure it has the following lines in the `redpanda` section:

```
enable_coproc: true
developer_mode: true
enable_idempotence: false
```

Then restart Redpanda with: `systemctl restart redpanda`

## Generating the data transform package

The data transform is packaged in a Node.js project and uses the WebAssembly (Wasm) instruction format.
We provide you with a project template that you can use to create your own transforms.

To create the base project, run: `rpk wasm generate <project_name>`

For example, let's create a project that will change the text in your events to all uppercase:

`rpk wasm generate uppercase`

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

## Add the transform logic

Edit the `/src/main.js` file and replace the default transform logic with your JavaScript.
The coprocessor API contains two main methods you must implement.
The first is a method called `subscribe()` which accepts an array of 2-tuples, and the second is the transform itself called `processRecord()` that is passed a recordBatch and expects a `Map<string, Array<recordBatch>>` as the return value,
where the key is your resultant topic name.

Remember that the batch API is Bytes-In-Bytes-Out.
We highly recommend that you build deterministic functions based on the input to make it easy to debug your applications.

Just for an example, the base project already contains JavaScript to make all of the data in your events uppercase:

```js
const uppercase = (record) => {
  const newRecord = {
    ...record,
    value: record.value.map((char) => {
      if (char >= 97 && char &lt;= 122) {
        return char - 32;
      } else {
        return char;
      }
    }),
  };
  return newRecord;
}
```

If your transform requires Node.js dependencies, add them to the `/src/package.json` file.


## Specify the source and destination topics

Also in the main.js file, find these lines and edit them to specify the source and destination topics:

- `transform.subscribe([["test-topic", PolicyInjection.Stored]]);` - Replace `test-topic` with the name of the topic
    that you want to consume events from.
	To add multiple source topics, add the topic and policy as pairs:
    `transform.subscribe[[<topic>,<policy>],[<topic>,<policy>]]`
- The return value of apply is `Map<string,<record_batch>>`.
- `result.set("result", transformedRecord);` - Replace `result` with the name of the topic
    that you want to produce the transformed events to.
    To produce onto more than one destination topic, add another line in the format:
    `result.set("<destination_topic>", transformedRecord);`

The destination topic is created with the name format of: `<source>._<destination>_`

## Prepare the script for deployment

Because the transform is packaged in a Node.js project, you need to install the dependencies and build the script that runs the transform.

To do this, run these commands in the project directory:

```bash
npm install
npm run build
```

The build command creates the `main.js` JavaScript file in the `/dist` directory that contains the compiled transform bundle.

## Deploy the transform

To get the transform to start consuming and producing events, you need to deploy it in redpanda
with a name and description.

As with other rpk commands, you must specify the brokers in the cluster and all of the authentication parameters
(including user, password, TLS) for the brokers.

> Note: If you haven't yet, create the source topics that the transform consumes from. In our example, that is:
> `rpk topic create test-topic`
> Destination topics are created automatically during script deployment.
> If you deploy the script to use existing topics, the deployment fails.

To deploy the sample transform, run:

`rpk wasm deploy uppercase/dist/main.js --name uppercase --description "Converts uppercase text to lowercase"`

## Verify that the transform works

After the transform is deployed, Redpanda processes every event produced to the source topic
and runs the logic that is defined by the transform.

To see our sample transform in action:

1. Run: `rpk topic consume test_topic._result_`
2. Produce events to the source topic:
    1. In a second terminal run: `rpk topic produce test-topic`
    2. Enter text and press CTRL+D to send the event to the source topic.
3. In the terminal that shows the consumed events you’ll see the text that you produced but it is now formatted with uppercase characters.

## Clean up the transform

To stop a transform, you have to remove the transform from the cluster brokers.

For our sample transform, run: `rpk wasm remove uppercase`

The transform stops processing events and is removed.

## Troubleshooting

The default location of the data transforms log file is: `/var/lib/redpanda/coprocessor/wasm_engine.log`. You can configure the location of this file in the [redpanda configuration file](/docs/configuration):

```
coproc_engine:
  logFilePath: <file_path>
```

If you need more help working with data transforms, join our [Slack](https://vectorized.io/slack) community and post your questions.
We're happy to help you succeed.

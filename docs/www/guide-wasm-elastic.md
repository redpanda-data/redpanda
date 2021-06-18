---
title: Transforming data with WebAssembly (Wasm) - Elasticsearch tutorial
order: 0
---
# Transforming data with WebAssembly (Wasm)

When your application components send records to Redpanda,
sometimes those records need to be transformed in the process.
Instead of adding another process to your application that consumes, transforms, and produces the records back to Redpanda,
you can run Wasm transformations inside Redpanda.

In general, the steps for transforming data on-the-fly in Redpanda are:

1. Create a Wasm script.
2. Deploy the script using the `rpk wasm` command.
3. Publish records to Redpanda.

In this tutorial, we'll use Redpanda installed on Linux to transform data and send it to Elasticsearch®.

## Prerequisites

For this tutorial you need:

- Redpanda v21.4.14 or higher on [Linux](https://vectorized.io/docs/quick-start-linux)
- Elasticsearch 7.10.2 or higher [running in a docker image](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#docker-cli-run-dev-mode)
- [NodeJS](https://nodejs.org/en/download/) 12.x or higher

## Enable Wasm engine on Redpanda

By default the Wasm engine is disabled, so the first thing we need to do is enable it.

To enable the Wasm engine:

1. Set the `redpanda.enable_coproc` value in the Redpanda config to `true`:

    ```bash
    sudo rpk redpanda config set 'redpanda.enable_coproc' true
    ```

2. Set the `developer_mode` to `true`:

    ```bash
    sudo rpk redpanda mode dev
    ```

3. Make sure that those changes are shown in the configuration file in `/etc/redpanda/redpanda.yaml`:

    ```bash
    cat /etc/redpanda/redpanda.yaml | grep developer_mode && \
    cat /etc/redpanda/redpanda.yaml | grep enable_coproc
    ```

    The result should be:

    ```bash
    $ cat /etc/redpanda/redpanda.yaml | grep developer_mode              
    developer_mode: true

    $ cat /etc/redpanda/redpanda.yaml | grep enable_coproc
    coproc_enable: true
    ```

4. Restart Redpanda with:

    ```
    sudo systemctl restart redpanda
    ```

## Create the Wasm script

To create a Wasm script we'll use the `rpk wasm generate` command.
This command creates a directory with a template that gives us a simple build system and basic example script.

1. Generate the Wasm template:

    ```bash
    rpk wasm generate hello-elastic
    ```

    Now we have a directory with an example Wasm script.
    Let's move to the `hello-elastic` directory and see what's there.

    ```bash
    cd hello-elastic && \
    tree .
    ```

    You should see these files in the `hello-elastic` directory.

    ```
      .
      ├── package.json
      ├── src
      │   └── main.js
      ├── test
      │   └── main.test.js
      └── webpack.js
    ```

3. Create a file named `src/elastic.js` to hold the transform script:

    ```bash
    touch src/elastic.js
    ```

4. Add Elasticsearch to your install dependencies:

    ```bash
    npm install @elastic/elasticsearch
    ```

5. In your editor of choice, put the javascript code for the transform into the `src/elastic.js` file:

    ```js
    const {
      SimpleTransform,
      PolicyError,
    } = require("@vectorizedio/wasm-api");

    const {Client} = require('@elastic/elasticsearch')

    const client = new Client({ node: 'http://localhost:9200' })

    const transform = new SimpleTransform();
    /* Topics that trigger the transform function */
    transform.subscribe(["origin"]);
    /* The strategy the transform engine will use when handling errors */
    transform.errorHandler(PolicyError.Deregister);
    /* function for validating record has the correct header key and value  */
    const containHeader = (key, value, record) =>
      record.headers.some( header =>
        header.headerKey.equals(Buffer.from(key)) &&
        header.value.equals(Buffer.from(value))
      )

    /* Transform function */
    transform.processRecord((recordBatch) => {
      console.log("Applying Wasm process function")
      return Promise.all(
        recordBatch.records.map((record) => {
          if (containHeader("logService", "elastic", record)) {
            console.log("Index record value to Elasticsearch")
            return client.index({
              index: "result_wasm",
              type: "redpanda_wasm",
              body: {
                /*
                index the record value into Elasticsearch
                */
                record_message: record.value.toString()
              }
            })
          }
        })
      ).then(() => {
          /*
          return a map with recordBatch indexed to Elasticsearch
          */
          const result = new Map();
          result.set("elasticIndex", recordBatch)
          /* 
          Wasm processRecord function has to return a Map always,
          although that map is empty map
          */
          return result
      })
      /*
        Note: if the previous promise failed, the Wasm errorHandler is
        going to handle that error. You can use `deregister policy` to 
        remove this Wasm script from the Wasm engine.    
      */
    });

    exports["default"] = transform;
    ```

6. Bundle the project files into a single file with:

    ```bash
    #this command installs all nodejs dependecies
    npm install && \
    #this command generate the `dist/elastic.js` file.
    npm run build
    ```

## Deploy the Wasm function to Redpanda

Now we'll deploy a Wasm script to receive events to a specifc topic and process the transforms.

1. Create a topic to use for the transform:

    ```bash
    rpk topic create origin
    ```

2. Deploy the Wasm transform with:

    ```bash
    rpk wasm deploy dist/elastic.js
    ```

    The results shows:

    ```
    Sent record to partition 0 at offset 1 with timestamp 2021-02-01 18:18:15.734185538 -0500 -05 m=+0.053943881.
    ```    

    The Wasm engine has a log file located by default in `/var/lib/redpanda/coprocessor/logs/wasm`.
    To see the status of your function as reported by the Wasm engine look for log lines like:

    ```bash
    2021-03-09T14:33:42.367Z [server] info: request enable wasm script:  14103244480447969041
    2021-03-09T14:33:42.380Z [server] info: wasm script loaded on nodejs engine: 14103244480447969041
    ```

## Run data through the Wasm engine

Here's the real fun -- seeing the transform work.

1. To transform data, produce records to the `origin` topic:

    ```bash
    rpk topic produce origin -H logService:elastic -n 5
    ```

    We publish the records with `logService:elastic`
    because they are required by our Wasm script to pass the `containHeader` validation
    and go to the `processRecord` function on our Wasm script.

    You can see a log line in `/var/lib/redpanda/coprocessor/logs/wasm` that look like:

    ```
    2021-05-13T18:20:49.344Z [WasmScript] info: Applying Wasm process function
    2021-05-13T18:20:49.344Z [WasmScript] info: Index record value to Elasticsearch
    ```

## Verify Elasticsearch results

Now we need to validate that our Wasm script received the records and published them to Elasticsearch.
To do this, we'll create a query to Elasticsearch that returns every hit that has a  `result_wasm` index.

1. Save this query in a file names `elastic-query.js`:

    ```js
    const { Client } = require("@elastic/elasticsearch")

    const client = new Client({ node: 'http://localhost:9200' })

    client.search({
      index: 'result_wasm'
      size: 1000
    }).then((elasticResult) => {
      const results = elasticResult.body.hits.hits.map(record => record._source)
      console.log(results)
    })
    ```

2. Run the query with:

    ```
    node elastic-query.js
    ```

    The output is:

    ```bash
    $ node elastic-query.js
    [
      { record_message: 'Information from Redpanda + Wasm' },
      { record_message: 'Information from Redpanda + Wasm' },
      { record_message: 'Information from Redpanda + Wasm' },
      { record_message: 'Information from Redpanda + Wasm' },
      { record_message: 'Information from Redpanda + Wasm' }
    ]
    ```

The result shows that the 5 records that we produced with `rpk topic produce origin -H logService:elastic -n 5`,
where `-n` is the number of records that we published to the `origin` topic. 

Now you have a Wasm script with an Elasticsearch connection.
You can use this tutorial as a base for building Wasm transformations.
Try it out yourself and tell us what you think in our [Slack](https://vectorized.io/slack) community.

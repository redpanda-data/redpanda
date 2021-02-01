# Redpanda Wasm engine + Elasticsearch

The Wasm engine feature was introduced in version 21.2.1. In this guide, you'll
learn to how create a wasm script, how publish record from Redpanda
to Elasticsearch and how deploy a wasm script using `rpk wasm` command.

Requirements:

1. Redpanda version 21.2.1 or greater. You can check it by running `rpk version`.
2. Elasticsearch, in this example we use the official [docker image](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html)

>Note: this guide just work for linux enviroment

## Enable Wasm engine on Redpanda
Add `coproc_enable: true` in your redpanda.yaml file, under the `redpanda` 
section. You can do this manually or by running 

```bash
$ sudo rpk redpanda config set 'redpanda.enable_coproc' true
If redpanda is running, please restart it for the changes to take effect.
```

You will also need to set `developer_mode: true`. To do so, you can run

```bash
$ sudo rpk redpanda mode dev
Writing 'dev' mode defaults to '/etc/redpanda/redpanda.yaml'
```

Verify that your configuration is OK by checking your configuration 
file (located in `/etc/redpanda/redpanda.yaml` by default):

```bash
$ cat /etc/redpanda/redpanda.yaml | grep developer_mode              
developer_mode: true

$ cat /etc/redpanda/redpanda.yaml | grep enable_coproc
  coproc_enable: true
```



## Create the Wasm script
In this example we use a folder for save all Wasm scripts, `/wasm_example`

To create a Wasm script we will use `rpk wasm generate wasm-elastic`. It 
will create a template giving us a simple build system and basic example script.

```bash
$ rpk wasm generate wasm-elastic
npm created project in /wasm_example/wasm
``` 
That command creates a folder with an example Wasm
script in `/wasm_example/wasm-elastic`. we move to `wasm-elastic` folder.

```bash
$ cd wasm_example/wasm-elastic

$ tree . 
  .
  ├── package.json
  ├── src
  │   └── wasm.js
  ├── test
  │   └── wasm.test.js
  └── webpack.js
```
Now we just to go `src/wasm.js`, and rename to `src/elastic.js`

```bash
$ mv src/wasm.js src/elastic.js
```

>Note: the file name is very important, since it will be used to uniquely 
identify the function when we upload it to the cluster. 

```bash
$ tree src
 src
  └── elastic.js
```

 Be sure to add Elasticsearch to your install dependencies:

```bash
$ npm install @elastic/elasticsearch
```

Open `src/elastic.js` in your editor of choice and paste the 
following javascript code. We'll walk through each of the 
sections to better understand what they do and how they fit 
together.

```js
const {
  SimpleTransform,
  PolicyError,
} = require("@vectorizedio/wasm-api");

const {Client} = require('@elastic/elasticsearch')

const client = new Client({ node: 'http://localhost:9200' })

const transform = new SimpleTransform();
/* Topics that fire the transform function */
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
    Note: if the previous promise failed, the wasm errorHandler is
    going to handle that error, in this case, Deregister policy will
    remove this wasm script from wasm engine.    
  */
});

exports["default"] = transform;
```

after applying the new logic, we need to bundle everything into a single file.
For it, we can use 

```bash
#this command install all nodejs dependecies
$ npm install
#this command generate `dist/elastic.js` file.
$ npm run build
```

Before deploying this Wasm script, we need to create the `origin` topic
The statically defined input topic that this Wasm operator has defined in its metadata

```bash
rpk topic create origin
```

 ## Deploy the Wasm function to Redpanda

we use `rpk wasm deploy dist/elastic.js` in this case

```bash
$ rpk wasm deploy dist/elastic.js

Sent record to partition 0 at offset 1 with timestamp 2021-02-01 18:18:15.734185538 -0500 -05 m=+0.053943881.
```

To verify that the wasm script has successfully been deployed, 
you can look through the redpanda logs for a log line that looks like this
for check it.

on `redpanda logs`

```bash
INFO [shard 0] coproc - service.cc:101 - Request recieved to register coprocessor with id: 14103244480447969041
```

The Wasm engine itself also has a log file which is located by default in `/var/lib/redpanda/coprocessor/logs/wasm`. To see the status of your function as reported by the Wasm engine look for log lines like [this]

```bash
   2021-03-09T14:33:42.367Z [server] info: request enable wasm script:  14103244480447969041
   2021-03-09T14:33:42.380Z [server] info: wasm script loaded on nodejs engine: 14103244480447969041
```

## Run data through the Wasm engine

To transform data, push records onto the `origin` topic using any 
kafka client. In this example we will use the kafka client 
within `rpk`.

>Note: we need to publish the records with `logService:elastic` 
>headers in order to those records pass the `containHeader` 
>validation into `processRecord` function on our wasm script.  

```bash
$ rpk topic produce origin -H logService:elastic -n 5
```

In `/var/lib/redpanda/coprocessor/logs/wasm` you should see log line that look like [this]

```
Applying Wasm process function
Index record value to Elasticsearch
```

## Verify Elasticsearch results
Now we need to validate that our Wasm script, should take records 
and publish them to Elasticsearch. To do this, we can use the same 
Elasticsearch client for making a query to Elasticsearch, which 
query should take every hist that has a  `result_wasm` index.

Will create `elastic-query.js`.

```js
const { Client } = require("@elastic/elasticsearch")

const client = new Client({ node: 'http://localhost:9200' })

client.search({
  index: 'result_wasm'
}).then((elasticResult) => {
  const results = elasticResult.body.hits.hits.map(record => record._source)
  console.log(results)
})
```
 expected output:

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

We should see 5 items in the list, because we send 5 records when 
we use `$ rpk topic produce origin -H logService:elastic -n 5`, `-n` is the
number of record that we are going to publish on `origin` topic. 

Now you have a Wasm script with Elasticsearch connection, this is a 
basic example about how to take advantage of Redpanda and Wasm 
engine, try it yourself and share your knowledge.
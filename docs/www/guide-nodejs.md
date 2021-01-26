# Node.js Setup

If you haven't already, install Node by following the appropriate steps for
your OS [here](https://nodejs.org/en/download/package-manager/).

As you may have heard by now, Redpanda is Kafka® API-compatible, which means
that although Redpanda is relatively new, you can leverage the countless client
libraries created for Kafka® (if you find something that is not supported,
reach out to our team on our [slack](https://vectorized.io/slack)).
In this case we will use [kafkajs](https://kafka.js.org/).
```bash
#create and enter the project folder
mkdir redpanda-node
cd redpanda-node
#generate package.json
npm init
#install needed dependencies
npm i -D typescript
npm i -D @types/node
npm i kafkajs
npm i uuid
npm i -D @types/uuid
#generate tsconfig.json
tsc --init
```

## Setting up Redpanda and creating a topic

If you are on MacOS, check out [this post](https://vectorized.io/docs/quick-start-macos).
If you’re on linux, follow the instructions in the
[Quick start section](https://vectorized.io/docs/quick-start-linux)
Note: please follow only this section. If you do the production section, it
will optimize your machine for Redpanda, which might affect your experience
with desktop applications and other services. Be especially careful with
`sudo rpk tune all` you probably don’t want to run that on your personal
workstation.


After Redpanda is installed and running, you will create the first topic
```
$ rpk api topic create chat-room
  Created topic 'chat-room'. Partitions: 1, replicas: 1, configuration:
  'cleanup.policy':'delete'
```
This will create a topic named `chat-room`, with one partition and one replica.
You can also see all created topics with:
```
$ rpk api topic list
  Name       Partitions  Replicas  
  chat-room  1           1
```

With that out of the way, we can get started. To have a working chat we need to
receive and send messages, so we need to create a consumer and a producer.

## Producer Setup

```typescript
// src/producer.ts
import {Kafka} from 'kafkajs';

const kafka = new Kafka({
  clientId: 'chat-app',
  brokers: ['127.0.0.1:9092']
});

const producer = kafka.producer();

export function getConnection(user: string){
  return producer.connect().then(() => {
    return (message: string) => {
      return producer.send({
        topic: 'chat-room', // the topic created before
        messages: [//we send the message and the user who sent it
          {value: JSON.stringify({message, user})},
        ],
      })
    }
  })
}

export function disconnect(){
  return producer.disconnect()
}
```
That’s it, a working producer, sending strings entered by the user. Keepin mind
that you can send buffers, meaning you can send pretty much anything you want.

## Consumer Setup

```typescript
// src/consumer.ts
import { v4 as uuidv4 } from 'uuid';
import {Kafka} from 'kafkajs';

const kafka = new Kafka({
  clientId: 'chat-app',
  brokers: ['127.0.0.1:9092']
});

const consumer = kafka.consumer({ groupId: uuidv4() }); // we need a unique groupId I'll explain down

export  function connect() {
  return consumer.connect().then(() =>
    consumer.subscribe({topic: 'chat-room'}).then(() =>
      consumer.run({
        eachMessage: async ({topic, partition, message}) => {
          const formattedValue = JSON.parse((message.value as Buffer).toString()); // everything comes as a buffer
          console.log(`${formattedValue.user}: ${formattedValue.message}`)// print the message
        },
      })
    )
  );
}

export function disconnect() {
  consumer.disconnect();
}
```
There you have it. This will get all produced messages and print them. You can
have as many consumer groups as you want, but bear in mind that each group will
get a message only once. That’s why we have the uuid generated.

## Putting everything together

```typescript
//src/index.ts
import readline from 'readline';

import * as Producer from './producer';
import * as Consumer from './consumer';

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

function start() {
  console.log('connecting...')
  Consumer.connect().then(() => {
    rl.question('enter user name \n', function (username) { // the username to print it along with the messages
      Producer.getConnection(username).then((sendMessage) => {
        console.log('connected, press Ctrl+C to exit')
        rl.on('line', (input) => {
          readline.moveCursor(process.stdout, 0,-1); // removing the input so you don't get duplicated items in terminal
          sendMessage(input);
        })
      })
    });
  })
}

start();
// handling shut down

process.on('SIGINT', process.exit);

process.on('exit', () => {
  Producer.disconnect();
  Consumer.disconnect();
  rl.close();
});
``` 

## Running

```
tsc && node src/index.js
```
Run this as many times as you want clients. At least 2 so you can chat between
2 terminals.

## Wrapping up

Now you have the basic building blocks to work with Redpanda. Try it yourself,
there are endless use cases - what we built here was just the simplest of
examples.
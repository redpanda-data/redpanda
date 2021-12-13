---
title: How events improve your app - An intro to events and streaming
order: 0
---

# How events improve your app: An intro to events and streaming

Components in distributed systems depend on each other for data and system updates,
and in time-sensitive applications the updates need to happen as close to wire speed as possible.
We'll focus on two particular ways that these updates are handled in software architecture:

- Messages - Updates that are sent directly from one component to another in order to trigger an action
- Events - Updates that indicate that an action occurred at a specific time and are not directed to any specific recipient

Both of these packages of information are generated for processing, manipulation, or storage.
To understand the benefits of event-driven architecture, let’s dive into some background.

## What are events and why use them?

An event is the details that describe an action that occurred at a specific time,
for example the purchase of a product in an online store.
The purchase event is defined by product, payment, and delivery as well as the time that the purchase occurred.
The purchase action, or event, was done in the purchasing component of the system,
but has an impact in many other components, such as inventory, payment processing, and shipping.

In event-driven architecture, all actions in the system are defined and packaged as events 
to precisely identify the individual actions and how they are processed throughout the system. 

Instead of processing updates one after the other in a serial fashion and limiting the system's performance to its worst components,
event-driven architecture allows the components to process events at their own pace.
This architecture helps developers to build systems that are fast and scalable.

## Event processing

Some components of the distributed system produce events as a result of a specific action that is done in that component.
These components are referred to as “producers."
When producers send these events and the events are read or stored in sequence,
these events represent a replayable log of changes in the system, also called a "stream."

A single event includes information that is required by one or many other components in the system, also known as “consumers”,
to effect additional changes.
The consumers can store, process, or react to these events.
Many times consumers also run processes that produce events for other components in the system,
so being a producer is not mutually exclusive from being a consumer.

In more traditional message-driven systems, data and system updates are sent as messages directly from the producer to the consumer.
The producer waits for acknowledgement that the consumer received the message before it continues with its processes.
This creates a few issues that can degrade the efficiency of the system.

|Message-driven|Event-driven|
|--- |--- |
|If similar information is required by multiple receipients, the sender must send a message designed for each receipient independently.|The producer sends individual events that are designed for consumption by multiple consumers.|
|If the receipient is delayed in acknowledging receipt, the producer can’t complete its process until it receives the acknowledgement.|The producer sends the events to an event processing system that can acknowledge receipt and guarantee delivery to the consumers.|
|If there is a break in the connection between a producer and a receipient, the producer doesn’t know if the event was processed or if it needs to resend the event.|The event processing system can track the communication between producers and consumers in the event of a broken connection.|
|In clustered deployments, each producer node has to send messages to each receipient node.|Each producer node sends events to the event processing system and each consumer node retrieves the events from that same system.|

When you use an event-processing system, like Redpanda, you decouple the producer from the consumer.
The event-processing system allows for asynchronous event processing, event tracking, event manipulation, and event archiving.

## Turning data into a product

Many of the benefits of an event-processing system can be found in a simple pub/sub process or database
that collects events from application components and makes them available to other components.
But what if you can implement it in a way that adds more value?

When you decouple your systems and set up an event-processing system to collect and route events,
you transform your event stream from individual actions into a warehouse of information about everything that happens in your application.
We call this a **data product**.

Every event that occurs in your system can be analyzed, mined, and transformed
to give you insight into your business and power to build new capabilities.

You can:

- Replay events from the past and route them to another process in your application
- Run transformations on the data in real-time or historically
- Integrate with other event processing systems that use the Kafka API

To take advantage of this precious resource, the event-processing system needs to be simple to run
and to leverage the speed of your hardware to handle the events as efficiently as possible.
Redpanda strives to give the simplicity and speed that you need to capitalize on the data product in your system.

Try it out!
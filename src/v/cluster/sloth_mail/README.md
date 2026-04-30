SlothMail is an RPC wrapper that batches non-urgent messages to reduce traffic.

Each message comprises destination node id, mail kind, key, value and shipping deadline. Key and value types are specific to the mail kind. If a message with the same destination, mail kind and key is dispatched before the original is shipped they are merged with the kind-specific merge function. The earliest deadline is used.

Shipping is triggered when either some mail to the destination is at the deadline, or there's too much mail buffered. It is a soft deadline: actual shipping may happen later, e.g. when there's already a request in flight to the same destination.

If a node leaves the cluster its incoming and outgoing mail may be permanently lost. Undelivered mail is also lost on node shutdown.

Mail kinds are configured in `kinds.h`. Adding, removing or incompatibly changing a mail kind must be guarded by feature flag, as failing to parse a message in a bundled RPC may result in losing unrelated mail.
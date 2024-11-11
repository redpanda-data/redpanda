### Intro

Request balancer is responsible for distributing the load between the shards
and for limiting resource usage.

Our previous approach was to measure the actual resource usage by every shard.
This complicates the design quite a lot.

The alternative to this approach is to implement load balancing as an external
service. The request balancer uses policy based approach. Currently, only the
simplest possible policy is implemented.
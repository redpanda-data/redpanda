This test

0. enables downscaling
1. creates a 3 node Redpanda cluster
2. waits for the Redpanda cluster to have 3 brokers ready
3. changes the cluster spec.replicas to 2
4. waits for the Redpanda cluster broker list to be reduced to 2 brokers
5. reset the operator to the default installation
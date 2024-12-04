This module contains interfaces used to interact with different subsystems in Redpanda.
The goal is to let all code in cloud_topics to use an interface and create an implementation of the interface here.
So for instance, we need to query the actual partition to read placeholder batches. The reader doesn't depend on
cluster and consumes a simple reader instead. But to get the reader we have to use `cluster::partition_manager`
and `cluster::partition`. To avoid direct dependencies we will introduce an interface that accepts log reader config
and NTP and returns a log reader. The implementation of this interface will be using read `cluster::partition_manager'
and `cluster::partition` under the hood.
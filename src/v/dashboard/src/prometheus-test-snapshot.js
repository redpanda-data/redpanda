export default `
# HELP vectorized_alien_receive_batch_queue_length Current receive batch queue length
# TYPE vectorized_alien_receive_batch_queue_length gauge
vectorized_alien_receive_batch_queue_length{shard="0"} 0.000000
vectorized_alien_receive_batch_queue_length{shard="1"} 0.000000
# HELP vectorized_alien_total_received_messages Total number of received messages
# TYPE vectorized_alien_total_received_messages counter
vectorized_alien_total_received_messages{shard="0"} 0
vectorized_alien_total_received_messages{shard="1"} 0
# HELP vectorized_alien_total_sent_messages Total number of sent messages
# TYPE vectorized_alien_total_sent_messages counter
vectorized_alien_total_sent_messages{shard="0"} 0
vectorized_alien_total_sent_messages{shard="1"} 0
# HELP vectorized_application_uptime Redpanda uptime in milliseconds
# TYPE vectorized_application_uptime gauge
vectorized_application_uptime{shard="0"} 12964954.000000
# HELP vectorized_partition_committed_offset Committed offset
# TYPE vectorized_partition_committed_offset gauge
vectorized_partition_committed_offset{namespace="redpanda",partition="0",shard="0",topic="controller"} 4.000000
# HELP vectorized_partition_last_stable_offset Last stable offset
# TYPE vectorized_partition_last_stable_offset gauge
vectorized_partition_last_stable_offset{namespace="redpanda",partition="0",shard="0",topic="controller"} 5.000000
# HELP vectorized_partition_leader Flag indicating if this partition instance is a leader
# TYPE vectorized_partition_leader gauge
vectorized_partition_leader{namespace="redpanda",partition="0",shard="0",topic="controller"} 1.000000
# HELP vectorized_partition_records_fetched Total number of records fetched
# TYPE vectorized_partition_records_fetched counter
vectorized_partition_records_fetched{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_partition_records_produced Total number of records produced
# TYPE vectorized_partition_records_produced counter
vectorized_partition_records_produced{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_httpd_connections_current The current number of open  connections
# TYPE vectorized_httpd_connections_current gauge
vectorized_httpd_connections_current{service="admin",shard="0"} 2.000000
vectorized_httpd_connections_current{service="admin",shard="1"} 2.000000
# HELP vectorized_httpd_connections_total The total number of connections opened
# TYPE vectorized_httpd_connections_total counter
vectorized_httpd_connections_total{service="admin",shard="0"} 108
vectorized_httpd_connections_total{service="admin",shard="1"} 11
# HELP vectorized_httpd_read_errors The total number of errors while reading http requests
# TYPE vectorized_httpd_read_errors counter
vectorized_httpd_read_errors{service="admin",shard="0"} 97
vectorized_httpd_read_errors{service="admin",shard="1"} 4
# HELP vectorized_httpd_reply_errors The total number of errors while replying to http
# TYPE vectorized_httpd_reply_errors counter
vectorized_httpd_reply_errors{service="admin",shard="0"} 0
vectorized_httpd_reply_errors{service="admin",shard="1"} 0
# HELP vectorized_httpd_requests_served The total number of http requests served
# TYPE vectorized_httpd_requests_served counter
vectorized_httpd_requests_served{service="admin",shard="0"} 1012
vectorized_httpd_requests_served{service="admin",shard="1"} 676
# HELP vectorized_internal_rpc_active_connections internal_rpc: Currently active connections
# TYPE vectorized_internal_rpc_active_connections gauge
vectorized_internal_rpc_active_connections{shard="0"} 1.000000
vectorized_internal_rpc_active_connections{shard="1"} 1.000000
# HELP vectorized_internal_rpc_connection_close_errors internal_rpc: Number of errors when shutting down the connection
# TYPE vectorized_internal_rpc_connection_close_errors counter
vectorized_internal_rpc_connection_close_errors{shard="0"} 0
vectorized_internal_rpc_connection_close_errors{shard="1"} 0
# HELP vectorized_internal_rpc_connects internal_rpc: Number of accepted connections
# TYPE vectorized_internal_rpc_connects counter
vectorized_internal_rpc_connects{shard="0"} 1
vectorized_internal_rpc_connects{shard="1"} 1
# HELP vectorized_internal_rpc_consumed_mem_bytes internal_rpc: Memory consumed by request processing
# TYPE vectorized_internal_rpc_consumed_mem_bytes counter
vectorized_internal_rpc_consumed_mem_bytes{shard="0"} 0
vectorized_internal_rpc_consumed_mem_bytes{shard="1"} 0
# HELP vectorized_internal_rpc_corrupted_headers internal_rpc: Number of requests with corrupted headers
# TYPE vectorized_internal_rpc_corrupted_headers counter
vectorized_internal_rpc_corrupted_headers{shard="0"} 0
vectorized_internal_rpc_corrupted_headers{shard="1"} 0
# HELP vectorized_internal_rpc_dispatch_handler_latency internal_rpc: Latency 
# TYPE vectorized_internal_rpc_dispatch_handler_latency histogram
vectorized_internal_rpc_dispatch_handler_latency_sum{shard="0"} 1.5836e+06
vectorized_internal_rpc_dispatch_handler_latency_count{shard="0"} 2
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 10
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 20
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 40
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 80
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 160
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 320
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 640
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 1280
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 2560
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 5120
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 10240
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 20480
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 40960
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 163840
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 327680
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 655360
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 1310720
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 2621440
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 2621440
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 2621440
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 2621440
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 2621440
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 2621440
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 2621440
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 2621440
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="+Inf",shard="0"} 2
vectorized_internal_rpc_dispatch_handler_latency_sum{shard="1"} 41749
vectorized_internal_rpc_dispatch_handler_latency_count{shard="1"} 3
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 10
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 20
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 40
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 80
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 160
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 320
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 640
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 1280
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 2560
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 5120
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 10240
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 20480
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 40960
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 81920
vectorized_internal_rpc_dispatch_handler_latency_bucket{le="+Inf",shard="1"} 3
# HELP vectorized_internal_rpc_max_service_mem_bytes internal_rpc: Maximum memory allowed for RPC
# TYPE vectorized_internal_rpc_max_service_mem_bytes counter
vectorized_internal_rpc_max_service_mem_bytes{shard="0"} 2157340262
vectorized_internal_rpc_max_service_mem_bytes{shard="1"} 2157340262
# HELP vectorized_internal_rpc_method_not_found_errors internal_rpc: Number of requests with not available RPC method
# TYPE vectorized_internal_rpc_method_not_found_errors counter
vectorized_internal_rpc_method_not_found_errors{shard="0"} 0
vectorized_internal_rpc_method_not_found_errors{shard="1"} 0
# HELP vectorized_internal_rpc_received_bytes internal_rpc: Number of bytes received from the clients in valid requests
# TYPE vectorized_internal_rpc_received_bytes counter
vectorized_internal_rpc_received_bytes{shard="0"} 113
vectorized_internal_rpc_received_bytes{shard="1"} 200
# HELP vectorized_internal_rpc_requests_blocked_memory internal_rpc: Number of requests blocked in memory backpressure
# TYPE vectorized_internal_rpc_requests_blocked_memory counter
vectorized_internal_rpc_requests_blocked_memory{shard="0"} 0
vectorized_internal_rpc_requests_blocked_memory{shard="1"} 0
# HELP vectorized_internal_rpc_requests_completed internal_rpc: Number of successfull requests
# TYPE vectorized_internal_rpc_requests_completed counter
vectorized_internal_rpc_requests_completed{shard="0"} 2
vectorized_internal_rpc_requests_completed{shard="1"} 3
# HELP vectorized_internal_rpc_requests_pending internal_rpc: Number of requests being processed by server
# TYPE vectorized_internal_rpc_requests_pending gauge
vectorized_internal_rpc_requests_pending{shard="0"} 0.000000
vectorized_internal_rpc_requests_pending{shard="1"} 0.000000
# HELP vectorized_internal_rpc_sent_bytes internal_rpc: Number of bytes sent to clients
# TYPE vectorized_internal_rpc_sent_bytes counter
vectorized_internal_rpc_sent_bytes{shard="0"} 100
vectorized_internal_rpc_sent_bytes{shard="1"} 127
# HELP vectorized_internal_rpc_service_errors internal_rpc: Number of service errors
# TYPE vectorized_internal_rpc_service_errors counter
vectorized_internal_rpc_service_errors{shard="0"} 0
vectorized_internal_rpc_service_errors{shard="1"} 0
# HELP vectorized_io_queue_delay total delay time in the queue
# TYPE vectorized_io_queue_delay gauge
vectorized_io_queue_delay{class="default",ioshard="0",mountpoint="none",shard="0"} 0.000002
vectorized_io_queue_delay{class="raft",ioshard="0",mountpoint="none",shard="0"} 0.000028
# HELP vectorized_io_queue_queue_length Number of requests in the queue
# TYPE vectorized_io_queue_queue_length gauge
vectorized_io_queue_queue_length{class="default",ioshard="0",mountpoint="none",shard="0"} 0.000000
vectorized_io_queue_queue_length{class="raft",ioshard="0",mountpoint="none",shard="0"} 0.000000
# HELP vectorized_io_queue_shares current amount of shares
# TYPE vectorized_io_queue_shares gauge
vectorized_io_queue_shares{class="default",ioshard="0",mountpoint="none",shard="0"} 1.000000
vectorized_io_queue_shares{class="raft",ioshard="0",mountpoint="none",shard="0"} 1000.000000
# HELP vectorized_io_queue_total_bytes Total bytes passed in the queue
# TYPE vectorized_io_queue_total_bytes counter
vectorized_io_queue_total_bytes{class="default",ioshard="0",mountpoint="none",shard="0"} 57344
vectorized_io_queue_total_bytes{class="raft",ioshard="0",mountpoint="none",shard="0"} 20480
# HELP vectorized_io_queue_total_operations Total bytes passed in the queue
# TYPE vectorized_io_queue_total_operations counter
vectorized_io_queue_total_operations{class="default",ioshard="0",mountpoint="none",shard="0"} 14
vectorized_io_queue_total_operations{class="raft",ioshard="0",mountpoint="none",shard="0"} 5
# HELP vectorized_kafka_rpc_active_connections kafka_rpc: Currently active connections
# TYPE vectorized_kafka_rpc_active_connections gauge
vectorized_kafka_rpc_active_connections{shard="0"} 0.000000
vectorized_kafka_rpc_active_connections{shard="1"} 0.000000
# HELP vectorized_kafka_rpc_connection_close_errors kafka_rpc: Number of errors when shutting down the connection
# TYPE vectorized_kafka_rpc_connection_close_errors counter
vectorized_kafka_rpc_connection_close_errors{shard="0"} 0
vectorized_kafka_rpc_connection_close_errors{shard="1"} 0
# HELP vectorized_kafka_rpc_connects kafka_rpc: Number of accepted connections
# TYPE vectorized_kafka_rpc_connects counter
vectorized_kafka_rpc_connects{shard="0"} 53
vectorized_kafka_rpc_connects{shard="1"} 35
# HELP vectorized_kafka_rpc_consumed_mem_bytes kafka_rpc: Memory consumed by request processing
# TYPE vectorized_kafka_rpc_consumed_mem_bytes counter
vectorized_kafka_rpc_consumed_mem_bytes{shard="0"} 0
vectorized_kafka_rpc_consumed_mem_bytes{shard="1"} 0
# HELP vectorized_kafka_rpc_corrupted_headers kafka_rpc: Number of requests with corrupted headers
# TYPE vectorized_kafka_rpc_corrupted_headers counter
vectorized_kafka_rpc_corrupted_headers{shard="0"} 0
vectorized_kafka_rpc_corrupted_headers{shard="1"} 0
# HELP vectorized_kafka_rpc_dispatch_handler_latency kafka_rpc: Latency 
# TYPE vectorized_kafka_rpc_dispatch_handler_latency histogram
vectorized_kafka_rpc_dispatch_handler_latency_sum{shard="0"} 1320
vectorized_kafka_rpc_dispatch_handler_latency_count{shard="0"} 87
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 10
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 20
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="0"} 80
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="+Inf",shard="0"} 87
vectorized_kafka_rpc_dispatch_handler_latency_sum{shard="1"} 611
vectorized_kafka_rpc_dispatch_handler_latency_count{shard="1"} 41
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 10
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 20
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="4611686018427387904.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="0.000000",shard="1"} 40
vectorized_kafka_rpc_dispatch_handler_latency_bucket{le="+Inf",shard="1"} 41
# HELP vectorized_kafka_rpc_max_service_mem_bytes kafka_rpc: Maximum memory allowed for RPC
# TYPE vectorized_kafka_rpc_max_service_mem_bytes counter
vectorized_kafka_rpc_max_service_mem_bytes{shard="0"} 2157340262
vectorized_kafka_rpc_max_service_mem_bytes{shard="1"} 2157340262
# HELP vectorized_kafka_rpc_method_not_found_errors kafka_rpc: Number of requests with not available RPC method
# TYPE vectorized_kafka_rpc_method_not_found_errors counter
vectorized_kafka_rpc_method_not_found_errors{shard="0"} 0
vectorized_kafka_rpc_method_not_found_errors{shard="1"} 0
# HELP vectorized_kafka_rpc_received_bytes kafka_rpc: Number of bytes received from the clients in valid requests
# TYPE vectorized_kafka_rpc_received_bytes counter
vectorized_kafka_rpc_received_bytes{shard="0"} 1532
vectorized_kafka_rpc_received_bytes{shard="1"} 732
# HELP vectorized_kafka_rpc_requests_blocked_memory kafka_rpc: Number of requests blocked in memory backpressure
# TYPE vectorized_kafka_rpc_requests_blocked_memory counter
vectorized_kafka_rpc_requests_blocked_memory{shard="0"} 0
vectorized_kafka_rpc_requests_blocked_memory{shard="1"} 0
# HELP vectorized_kafka_rpc_requests_completed kafka_rpc: Number of successfull requests
# TYPE vectorized_kafka_rpc_requests_completed counter
vectorized_kafka_rpc_requests_completed{shard="0"} 87
vectorized_kafka_rpc_requests_completed{shard="1"} 41
# HELP vectorized_kafka_rpc_requests_pending kafka_rpc: Number of requests being processed by server
# TYPE vectorized_kafka_rpc_requests_pending gauge
vectorized_kafka_rpc_requests_pending{shard="0"} 0.000000
vectorized_kafka_rpc_requests_pending{shard="1"} 0.000000
# HELP vectorized_kafka_rpc_sent_bytes kafka_rpc: Number of bytes sent to clients
# TYPE vectorized_kafka_rpc_sent_bytes counter
vectorized_kafka_rpc_sent_bytes{shard="0"} 13720
vectorized_kafka_rpc_sent_bytes{shard="1"} 7448
# HELP vectorized_kafka_rpc_service_errors kafka_rpc: Number of service errors
# TYPE vectorized_kafka_rpc_service_errors counter
vectorized_kafka_rpc_service_errors{shard="0"} 0
vectorized_kafka_rpc_service_errors{shard="1"} 0
# HELP vectorized_memory_allocated_memory Allocated memeory size in bytes
# TYPE vectorized_memory_allocated_memory counter
vectorized_memory_allocated_memory{shard="0"} 76328960
vectorized_memory_allocated_memory{shard="1"} 71442432
# HELP vectorized_memory_cross_cpu_free_operations Total number of cross cpu free
# TYPE vectorized_memory_cross_cpu_free_operations counter
vectorized_memory_cross_cpu_free_operations{shard="0"} 85794
vectorized_memory_cross_cpu_free_operations{shard="1"} 85799
# HELP vectorized_memory_free_memory Free memeory size in bytes
# TYPE vectorized_memory_free_memory counter
vectorized_memory_free_memory{shard="0"} 7114805248
vectorized_memory_free_memory{shard="1"} 7119691776
# HELP vectorized_memory_free_operations Total number of free operations
# TYPE vectorized_memory_free_operations counter
vectorized_memory_free_operations{shard="0"} 15135579378
vectorized_memory_free_operations{shard="1"} 1866975673
# HELP vectorized_memory_malloc_live_objects Number of live objects
# TYPE vectorized_memory_malloc_live_objects gauge
vectorized_memory_malloc_live_objects{shard="0"} 6139.000000
vectorized_memory_malloc_live_objects{shard="1"} 2542.000000
# HELP vectorized_memory_malloc_operations Total number of malloc operations
# TYPE vectorized_memory_malloc_operations counter
vectorized_memory_malloc_operations{shard="0"} 15135585518
vectorized_memory_malloc_operations{shard="1"} 1866978216
# HELP vectorized_memory_reclaims_operations Total reclaims operations
# TYPE vectorized_memory_reclaims_operations counter
vectorized_memory_reclaims_operations{shard="0"} 0
vectorized_memory_reclaims_operations{shard="1"} 0
# HELP vectorized_memory_total_memory Total memeory size in bytes
# TYPE vectorized_memory_total_memory counter
vectorized_memory_total_memory{shard="0"} 7191134208
vectorized_memory_total_memory{shard="1"} 7191134208
# HELP vectorized_raft_done_replicate_requests Number of finished replicate requests
# TYPE vectorized_raft_done_replicate_requests counter
vectorized_raft_done_replicate_requests{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_raft_group_count Number of raft groups
# TYPE vectorized_raft_group_count gauge
vectorized_raft_group_count{shard="0"} 1.000000
vectorized_raft_group_count{shard="1"} 0.000000
# HELP vectorized_raft_leader_for Number of groups for which node is a leader
# TYPE vectorized_raft_leader_for gauge
vectorized_raft_leader_for{namespace="redpanda",partition="0",shard="0",topic="controller"} 1.000000
# HELP vectorized_raft_leadership_changes Number of leadership changes
# TYPE vectorized_raft_leadership_changes counter
vectorized_raft_leadership_changes{namespace="redpanda",partition="0",shard="0",topic="controller"} 2
# HELP vectorized_raft_log_flushes Number of log flushes
# TYPE vectorized_raft_log_flushes counter
vectorized_raft_log_flushes{namespace="redpanda",partition="0",shard="0",topic="controller"} 5
# HELP vectorized_raft_log_truncations Number of log truncations
# TYPE vectorized_raft_log_truncations counter
vectorized_raft_log_truncations{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_raft_received_append_requests Number of append requests received
# TYPE vectorized_raft_received_append_requests counter
vectorized_raft_received_append_requests{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_raft_received_vote_requests Number of vote requests received
# TYPE vectorized_raft_received_vote_requests counter
vectorized_raft_received_vote_requests{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_raft_replicate_ack_all_requests Number of replicate requests with quorum ack consistency
# TYPE vectorized_raft_replicate_ack_all_requests counter
vectorized_raft_replicate_ack_all_requests{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_raft_replicate_ack_leader_requests Number of replicate requests with leader ack consistency
# TYPE vectorized_raft_replicate_ack_leader_requests counter
vectorized_raft_replicate_ack_leader_requests{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_raft_replicate_ack_none_requests Number of replicate requests with no ack consistency
# TYPE vectorized_raft_replicate_ack_none_requests counter
vectorized_raft_replicate_ack_none_requests{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_raft_sent_vote_requests Number of vote requests sent
# TYPE vectorized_raft_sent_vote_requests counter
vectorized_raft_sent_vote_requests{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_reactor_abandoned_failed_futures Total number of abandoned failed futures, futures destroyed while still containing an exception
# TYPE vectorized_reactor_abandoned_failed_futures counter
vectorized_reactor_abandoned_failed_futures{shard="0"} 0
vectorized_reactor_abandoned_failed_futures{shard="1"} 0
# HELP vectorized_reactor_aio_bytes_read Total aio-reads bytes
# TYPE vectorized_reactor_aio_bytes_read counter
vectorized_reactor_aio_bytes_read{shard="0"} 4096
vectorized_reactor_aio_bytes_read{shard="1"} 0
# HELP vectorized_reactor_aio_bytes_write Total aio-writes bytes
# TYPE vectorized_reactor_aio_bytes_write counter
vectorized_reactor_aio_bytes_write{shard="0"} 73728
vectorized_reactor_aio_bytes_write{shard="1"} 0
# HELP vectorized_reactor_aio_errors Total aio errors
# TYPE vectorized_reactor_aio_errors counter
vectorized_reactor_aio_errors{shard="0"} 0
vectorized_reactor_aio_errors{shard="1"} 0
# HELP vectorized_reactor_aio_reads Total aio-reads operations
# TYPE vectorized_reactor_aio_reads counter
vectorized_reactor_aio_reads{shard="0"} 1
vectorized_reactor_aio_reads{shard="1"} 0
# HELP vectorized_reactor_aio_writes Total aio-writes operations
# TYPE vectorized_reactor_aio_writes counter
vectorized_reactor_aio_writes{shard="0"} 18
vectorized_reactor_aio_writes{shard="1"} 0
# HELP vectorized_reactor_cpp_exceptions Total number of C++ exceptions
# TYPE vectorized_reactor_cpp_exceptions counter
vectorized_reactor_cpp_exceptions{shard="0"} 97
vectorized_reactor_cpp_exceptions{shard="1"} 4
# HELP vectorized_reactor_cpu_busy_ms Total cpu busy time in milliseconds
# TYPE vectorized_reactor_cpu_busy_ms counter
vectorized_reactor_cpu_busy_ms{shard="0"} 20852
vectorized_reactor_cpu_busy_ms{shard="1"} 10843
# HELP vectorized_reactor_cpu_steal_time_ms Total steal time, the time in which some other process was running while Seastar was not trying to run (not sleeping).Because this is in userspace, some time that could be legitimally thought as steal time is not accounted as such. For example, if we are sleeping and can wake up but the kernel hasn't woken us up yet.
# TYPE vectorized_reactor_cpu_steal_time_ms counter
vectorized_reactor_cpu_steal_time_ms{shard="0"} 18446744073709518913
vectorized_reactor_cpu_steal_time_ms{shard="1"} 18446744073709541470
# HELP vectorized_reactor_fstream_read_bytes Counts bytes read from disk file streams.  A high rate indicates high disk activity. Divide by fstream_reads to determine average read size.
# TYPE vectorized_reactor_fstream_read_bytes counter
vectorized_reactor_fstream_read_bytes{shard="0"} 0
vectorized_reactor_fstream_read_bytes{shard="1"} 0
# HELP vectorized_reactor_fstream_read_bytes_blocked Counts the number of bytes read from disk that could not be satisfied from read-ahead buffers, and had to block. Indicates short streams, or incorrect read ahead configuration.
# TYPE vectorized_reactor_fstream_read_bytes_blocked counter
vectorized_reactor_fstream_read_bytes_blocked{shard="0"} 0
vectorized_reactor_fstream_read_bytes_blocked{shard="1"} 0
# HELP vectorized_reactor_fstream_reads Counts reads from disk file streams.  A high rate indicates high disk activity. Contrast with other fstream_read* counters to locate bottlenecks.
# TYPE vectorized_reactor_fstream_reads counter
vectorized_reactor_fstream_reads{shard="0"} 0
vectorized_reactor_fstream_reads{shard="1"} 0
# HELP vectorized_reactor_fstream_reads_ahead_bytes_discarded Counts the number of buffered bytes that were read ahead of time and were discarded because they were not needed, wasting disk bandwidth. Indicates over-eager read ahead configuration.
# TYPE vectorized_reactor_fstream_reads_ahead_bytes_discarded counter
vectorized_reactor_fstream_reads_ahead_bytes_discarded{shard="0"} 0
vectorized_reactor_fstream_reads_ahead_bytes_discarded{shard="1"} 0
# HELP vectorized_reactor_fstream_reads_aheads_discarded Counts the number of times a buffer that was read ahead of time and was discarded because it was not needed, wasting disk bandwidth. Indicates over-eager read ahead configuration.
# TYPE vectorized_reactor_fstream_reads_aheads_discarded counter
vectorized_reactor_fstream_reads_aheads_discarded{shard="0"} 0
vectorized_reactor_fstream_reads_aheads_discarded{shard="1"} 0
# HELP vectorized_reactor_fstream_reads_blocked Counts the number of times a disk read could not be satisfied from read-ahead buffers, and had to block. Indicates short streams, or incorrect read ahead configuration.
# TYPE vectorized_reactor_fstream_reads_blocked counter
vectorized_reactor_fstream_reads_blocked{shard="0"} 0
vectorized_reactor_fstream_reads_blocked{shard="1"} 0
# HELP vectorized_reactor_fsyncs Total number of fsync operations
# TYPE vectorized_reactor_fsyncs counter
vectorized_reactor_fsyncs{shard="0"} 39
vectorized_reactor_fsyncs{shard="1"} 8
# HELP vectorized_reactor_io_threaded_fallbacks Total number of io-threaded-fallbacks operations
# TYPE vectorized_reactor_io_threaded_fallbacks counter
vectorized_reactor_io_threaded_fallbacks{shard="0"} 122
vectorized_reactor_io_threaded_fallbacks{shard="1"} 38
# HELP vectorized_reactor_logging_failures Total number of logging failures
# TYPE vectorized_reactor_logging_failures counter
vectorized_reactor_logging_failures{shard="0"} 0
vectorized_reactor_logging_failures{shard="1"} 0
# HELP vectorized_reactor_polls Number of times pollers were executed
# TYPE vectorized_reactor_polls counter
vectorized_reactor_polls{shard="0"} 15127767470
vectorized_reactor_polls{shard="1"} 1862125621
# HELP vectorized_reactor_tasks_pending Number of pending tasks in the queue
# TYPE vectorized_reactor_tasks_pending gauge
vectorized_reactor_tasks_pending{shard="0"} 0.000000
vectorized_reactor_tasks_pending{shard="1"} 0.000000
# HELP vectorized_reactor_tasks_processed Total tasks processed
# TYPE vectorized_reactor_tasks_processed counter
vectorized_reactor_tasks_processed{shard="0"} 2004179
vectorized_reactor_tasks_processed{shard="1"} 1306118
# HELP vectorized_reactor_timers_pending Number of tasks in the timer-pending queue
# TYPE vectorized_reactor_timers_pending counter
vectorized_reactor_timers_pending{shard="0"} 5
vectorized_reactor_timers_pending{shard="1"} 4
# HELP vectorized_reactor_utilization CPU utilization
# TYPE vectorized_reactor_utilization gauge
vectorized_reactor_utilization{shard="0"} 0.000000
vectorized_reactor_utilization{shard="1"} 0.000000
# HELP vectorized_rpc_client_active_connections Currently active connections
# TYPE vectorized_rpc_client_active_connections gauge
vectorized_rpc_client_active_connections{shard="0",target="172.31.14.46:33145"} 1.000000
vectorized_rpc_client_active_connections{shard="1",target="172.31.2.101:33145"} 1.000000
# HELP vectorized_rpc_client_client_correlation_errors Number of errors in client correlation id
# TYPE vectorized_rpc_client_client_correlation_errors counter
vectorized_rpc_client_client_correlation_errors{shard="0",target="172.31.14.46:33145"} 0
vectorized_rpc_client_client_correlation_errors{shard="1",target="172.31.2.101:33145"} 0
# HELP vectorized_rpc_client_connection_errors Number of connection errors
# TYPE vectorized_rpc_client_connection_errors counter
vectorized_rpc_client_connection_errors{shard="0",target="172.31.14.46:33145"} 0
vectorized_rpc_client_connection_errors{shard="1",target="172.31.2.101:33145"} 0
# HELP vectorized_rpc_client_connects Connection attempts
# TYPE vectorized_rpc_client_connects counter
vectorized_rpc_client_connects{shard="0",target="172.31.14.46:33145"} 1
vectorized_rpc_client_connects{shard="1",target="172.31.2.101:33145"} 1
# HELP vectorized_rpc_client_corrupted_headers Number of responses with corrupted headers
# TYPE vectorized_rpc_client_corrupted_headers counter
vectorized_rpc_client_corrupted_headers{shard="0",target="172.31.14.46:33145"} 0
vectorized_rpc_client_corrupted_headers{shard="1",target="172.31.2.101:33145"} 0
# HELP vectorized_rpc_client_in_bytes Total number of bytes sent (including headers)
# TYPE vectorized_rpc_client_in_bytes counter
vectorized_rpc_client_in_bytes{shard="0",target="172.31.14.46:33145"} 3346224
vectorized_rpc_client_in_bytes{shard="1",target="172.31.2.101:33145"} 3347795
# HELP vectorized_rpc_client_out_bytes Total number of bytes received
# TYPE vectorized_rpc_client_out_bytes counter
vectorized_rpc_client_out_bytes{shard="0",target="172.31.14.46:33145"} 3430333
vectorized_rpc_client_out_bytes{shard="1",target="172.31.2.101:33145"} 3432115
# HELP vectorized_rpc_client_read_dispatch_errors Number of errors while dispatching responses
# TYPE vectorized_rpc_client_read_dispatch_errors counter
vectorized_rpc_client_read_dispatch_errors{shard="0",target="172.31.14.46:33145"} 0
vectorized_rpc_client_read_dispatch_errors{shard="1",target="172.31.2.101:33145"} 0
# HELP vectorized_rpc_client_request_errors Number or requests errors
# TYPE vectorized_rpc_client_request_errors counter
vectorized_rpc_client_request_errors{shard="0",target="172.31.14.46:33145"} 0
vectorized_rpc_client_request_errors{shard="1",target="172.31.2.101:33145"} 0
# HELP vectorized_rpc_client_request_timeouts Number or requests timeouts
# TYPE vectorized_rpc_client_request_timeouts counter
vectorized_rpc_client_request_timeouts{shard="0",target="172.31.14.46:33145"} 1
vectorized_rpc_client_request_timeouts{shard="1",target="172.31.2.101:33145"} 0
# HELP vectorized_rpc_client_requests Number of requests
# TYPE vectorized_rpc_client_requests counter
vectorized_rpc_client_requests{shard="0",target="172.31.14.46:33145"} 85757
vectorized_rpc_client_requests{shard="1",target="172.31.2.101:33145"} 85799
# HELP vectorized_rpc_client_requests_blocked_memory Number of requests that are blocked beacause of insufficient memory
# TYPE vectorized_rpc_client_requests_blocked_memory counter
vectorized_rpc_client_requests_blocked_memory{shard="0",target="172.31.14.46:33145"} 0
vectorized_rpc_client_requests_blocked_memory{shard="1",target="172.31.2.101:33145"} 0
# HELP vectorized_rpc_client_requests_pending Number of requests pending
# TYPE vectorized_rpc_client_requests_pending gauge
vectorized_rpc_client_requests_pending{shard="0",target="172.31.14.46:33145"} 0.000000
vectorized_rpc_client_requests_pending{shard="1",target="172.31.2.101:33145"} 0.000000
# HELP vectorized_rpc_client_server_correlation_errors Number of responses with wrong correlation id
# TYPE vectorized_rpc_client_server_correlation_errors counter
vectorized_rpc_client_server_correlation_errors{shard="0",target="172.31.14.46:33145"} 1
vectorized_rpc_client_server_correlation_errors{shard="1",target="172.31.2.101:33145"} 0
# HELP vectorized_scheduler_queue_length Size of backlog on this queue, in tasks; indicates whether the queue is busy and/or contended
# TYPE vectorized_scheduler_queue_length gauge
vectorized_scheduler_queue_length{group="admin",shard="0"} 0.000000
vectorized_scheduler_queue_length{group="atexit",shard="0"} 0.000000
vectorized_scheduler_queue_length{group="cluster",shard="0"} 0.000000
vectorized_scheduler_queue_length{group="coproc",shard="0"} 0.000000
vectorized_scheduler_queue_length{group="kafka",shard="0"} 0.000000
vectorized_scheduler_queue_length{group="main",shard="0"} 0.000000
vectorized_scheduler_queue_length{group="raft",shard="0"} 0.000000
vectorized_scheduler_queue_length{group="admin",shard="1"} 0.000000
vectorized_scheduler_queue_length{group="atexit",shard="1"} 0.000000
vectorized_scheduler_queue_length{group="cluster",shard="1"} 0.000000
vectorized_scheduler_queue_length{group="coproc",shard="1"} 0.000000
vectorized_scheduler_queue_length{group="kafka",shard="1"} 0.000000
vectorized_scheduler_queue_length{group="main",shard="1"} 0.000000
vectorized_scheduler_queue_length{group="raft",shard="1"} 0.000000
# HELP vectorized_scheduler_runtime_ms Accumulated runtime of this task queue; an increment rate of 1000ms per second indicates full utilization
# TYPE vectorized_scheduler_runtime_ms counter
vectorized_scheduler_runtime_ms{group="admin",shard="0"} 1591
vectorized_scheduler_runtime_ms{group="atexit",shard="0"} 0
vectorized_scheduler_runtime_ms{group="cluster",shard="0"} 1474
vectorized_scheduler_runtime_ms{group="coproc",shard="0"} 0
vectorized_scheduler_runtime_ms{group="kafka",shard="0"} 0
vectorized_scheduler_runtime_ms{group="main",shard="0"} 1676
vectorized_scheduler_runtime_ms{group="raft",shard="0"} 0
vectorized_scheduler_runtime_ms{group="admin",shard="1"} 1069
vectorized_scheduler_runtime_ms{group="atexit",shard="1"} 0
vectorized_scheduler_runtime_ms{group="cluster",shard="1"} 1833
vectorized_scheduler_runtime_ms{group="coproc",shard="1"} 0
vectorized_scheduler_runtime_ms{group="kafka",shard="1"} 0
vectorized_scheduler_runtime_ms{group="main",shard="1"} 2638
vectorized_scheduler_runtime_ms{group="raft",shard="1"} 0
# HELP vectorized_scheduler_shares Shares allocated to this queue
# TYPE vectorized_scheduler_shares gauge
vectorized_scheduler_shares{group="admin",shard="0"} 100.000000
vectorized_scheduler_shares{group="atexit",shard="0"} 1000.000000
vectorized_scheduler_shares{group="cluster",shard="0"} 300.000000
vectorized_scheduler_shares{group="coproc",shard="0"} 100.000000
vectorized_scheduler_shares{group="kafka",shard="0"} 1000.000000
vectorized_scheduler_shares{group="main",shard="0"} 1000.000000
vectorized_scheduler_shares{group="raft",shard="0"} 1000.000000
vectorized_scheduler_shares{group="admin",shard="1"} 100.000000
vectorized_scheduler_shares{group="atexit",shard="1"} 1000.000000
vectorized_scheduler_shares{group="cluster",shard="1"} 300.000000
vectorized_scheduler_shares{group="coproc",shard="1"} 100.000000
vectorized_scheduler_shares{group="kafka",shard="1"} 1000.000000
vectorized_scheduler_shares{group="main",shard="1"} 1000.000000
vectorized_scheduler_shares{group="raft",shard="1"} 1000.000000
# HELP vectorized_scheduler_tasks_processed Count of tasks executing on this queue; indicates together with runtime_ms indicates length of tasks
# TYPE vectorized_scheduler_tasks_processed counter
vectorized_scheduler_tasks_processed{group="admin",shard="0"} 27105
vectorized_scheduler_tasks_processed{group="atexit",shard="0"} 0
vectorized_scheduler_tasks_processed{group="cluster",shard="0"} 857770
vectorized_scheduler_tasks_processed{group="coproc",shard="0"} 1
vectorized_scheduler_tasks_processed{group="kafka",shard="0"} 1
vectorized_scheduler_tasks_processed{group="main",shard="0"} 1119301
vectorized_scheduler_tasks_processed{group="raft",shard="0"} 1
vectorized_scheduler_tasks_processed{group="admin",shard="1"} 17795
vectorized_scheduler_tasks_processed{group="atexit",shard="1"} 0
vectorized_scheduler_tasks_processed{group="cluster",shard="1"} 858029
vectorized_scheduler_tasks_processed{group="coproc",shard="1"} 1
vectorized_scheduler_tasks_processed{group="kafka",shard="1"} 1
vectorized_scheduler_tasks_processed{group="main",shard="1"} 430291
vectorized_scheduler_tasks_processed{group="raft",shard="1"} 1
# HELP vectorized_scheduler_time_spent_on_task_quota_violations_ms Total amount in milliseconds we were in violation of the task quota
# TYPE vectorized_scheduler_time_spent_on_task_quota_violations_ms counter
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="admin",shard="0"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="atexit",shard="0"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="cluster",shard="0"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="coproc",shard="0"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="kafka",shard="0"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="main",shard="0"} 3
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="raft",shard="0"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="admin",shard="1"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="atexit",shard="1"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="cluster",shard="1"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="coproc",shard="1"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="kafka",shard="1"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="main",shard="1"} 0
vectorized_scheduler_time_spent_on_task_quota_violations_ms{group="raft",shard="1"} 0
# HELP vectorized_stall_detector_reported Total number of reported stalls, look in the traces for the exact reason
# TYPE vectorized_stall_detector_reported counter
vectorized_stall_detector_reported{shard="0"} 0
vectorized_stall_detector_reported{shard="1"} 0
# HELP vectorized_storage_kvstore_cached_bytes Size of the database in memory
# TYPE vectorized_storage_kvstore_cached_bytes counter
vectorized_storage_kvstore_cached_bytes{shard="0"} 1030
vectorized_storage_kvstore_cached_bytes{shard="1"} 0
# HELP vectorized_storage_kvstore_entries_fetched Number of entries fetched
# TYPE vectorized_storage_kvstore_entries_fetched counter
vectorized_storage_kvstore_entries_fetched{shard="0"} 7
vectorized_storage_kvstore_entries_fetched{shard="1"} 0
# HELP vectorized_storage_kvstore_entries_removed Number of entries removaled
# TYPE vectorized_storage_kvstore_entries_removed counter
vectorized_storage_kvstore_entries_removed{shard="0"} 0
vectorized_storage_kvstore_entries_removed{shard="1"} 0
# HELP vectorized_storage_kvstore_entries_written Number of entries written
# TYPE vectorized_storage_kvstore_entries_written counter
vectorized_storage_kvstore_entries_written{shard="0"} 14
vectorized_storage_kvstore_entries_written{shard="1"} 0
# HELP vectorized_storage_kvstore_key_count Number of keys in the database
# TYPE vectorized_storage_kvstore_key_count counter
vectorized_storage_kvstore_key_count{shard="0"} 4
vectorized_storage_kvstore_key_count{shard="1"} 0
# HELP vectorized_storage_kvstore_segments_rolled Number of segments rolled
# TYPE vectorized_storage_kvstore_segments_rolled counter
vectorized_storage_kvstore_segments_rolled{shard="0"} 0
vectorized_storage_kvstore_segments_rolled{shard="1"} 0
# HELP vectorized_storage_log_batch_parse_errors Number of batch parsing (reading) errors
# TYPE vectorized_storage_log_batch_parse_errors counter
vectorized_storage_log_batch_parse_errors{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_storage_log_batch_write_errors Number of batch write errors
# TYPE vectorized_storage_log_batch_write_errors counter
vectorized_storage_log_batch_write_errors{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_storage_log_batches_read Total number of batches read
# TYPE vectorized_storage_log_batches_read counter
vectorized_storage_log_batches_read{namespace="redpanda",partition="0",shard="0",topic="controller"} 11
# HELP vectorized_storage_log_batches_written Total number of batches written
# TYPE vectorized_storage_log_batches_written counter
vectorized_storage_log_batches_written{namespace="redpanda",partition="0",shard="0",topic="controller"} 5
# HELP vectorized_storage_log_cached_batches_read Total number of cached batches read
# TYPE vectorized_storage_log_cached_batches_read counter
vectorized_storage_log_cached_batches_read{namespace="redpanda",partition="0",shard="0",topic="controller"} 11
# HELP vectorized_storage_log_cached_read_bytes Total number of cached bytes read
# TYPE vectorized_storage_log_cached_read_bytes counter
vectorized_storage_log_cached_read_bytes{namespace="redpanda",partition="0",shard="0",topic="controller"} 3159
# HELP vectorized_storage_log_compacted_segment Number of compacted segments
# TYPE vectorized_storage_log_compacted_segment counter
vectorized_storage_log_compacted_segment{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_storage_log_corrupted_compaction_indices Number of times we had to re-construct the .compaction index on a segment
# TYPE vectorized_storage_log_corrupted_compaction_indices counter
vectorized_storage_log_corrupted_compaction_indices{namespace="redpanda",partition="0",shard="0",topic="controller"} 0
# HELP vectorized_storage_log_log_segments_created Number of created log segments
# TYPE vectorized_storage_log_log_segments_created counter
vectorized_storage_log_log_segments_created{namespace="redpanda",partition="0",shard="0",topic="controller"} 1
# HELP vectorized_storage_log_partition_size Current size of partition in bytes
# TYPE vectorized_storage_log_partition_size gauge
vectorized_storage_log_partition_size{namespace="redpanda",partition="0",shard="0",topic="controller"} 1163.000000
# HELP vectorized_storage_log_read_bytes Total number of bytes read
# TYPE vectorized_storage_log_read_bytes counter
vectorized_storage_log_read_bytes{namespace="redpanda",partition="0",shard="0",topic="controller"} 3159
# HELP vectorized_storage_log_written_bytes Total number of bytes written
# TYPE vectorized_storage_log_written_bytes counter
vectorized_storage_log_written_bytes{namespace="redpanda",partition="0",shard="0",topic="controller"} 1163
`;

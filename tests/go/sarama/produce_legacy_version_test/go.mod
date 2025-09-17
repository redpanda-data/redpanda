// This test case uses an old version of Sarama which does NOT properly set the max_timestamp in a batch.
// This version is useful for testing redpanda behavior with improper timestamps.
module produce_test

go 1.19

require github.com/Shopify/sarama v1.38.1

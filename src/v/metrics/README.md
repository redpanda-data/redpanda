# Adding metrics

Together with logs metrics are one of our core tools for observability and
debugability. Many incidents, bugs or performance issues can purely be solved
based on metrics. Hence adding new metrics to further improve our insight into
Redpanda is highly encouraged.

However we do already have a huge amount of metrics. Depending on certain
parameters such as partition count a prometheus scrape might have millions of
lines for certain clusters. This causes problems at scrape and ingestion time
which can ultimately lead to a total loss of metrics. 

Hence we need to be mindful of how many metrics we add. Below is a guide for
things to watch out for when adding new metrics. 

## Metrics Aggregation

One thing to keep in mind when adding new metrics is “metrics aggregation”. In
our context this refers to a seastar feature where on metric creation one can
specify “aggregation labels”. These are labels which will be “aggregated away”.
For example if you had N series of a metrics family all of which have a
different value for a certain label and you specified that label as an
aggregation label then the resulting prometheus output would have a single
series for that metric family with the label removed and the value being the sum
of all the aggregated series. 

Note above I put quotes around many instances of “aggregation” which is because
aggregation can mean the opposite in the prometheus sense. I.e.: if you do
something like a sum by (foo, bar) then actually all but foo and bar will be
“aggregated away”.

To solve the aforementioned problem of overloading our metrics system we employ
metric label aggregation to reduce the count of metrics series we produce.

In a few cases some metrics are defined with static aggregation labels. This
rarely makes sense as one could just avoid adding the extra labels in this case.
However the one exception is the shard label which gets automatically attached
to each metric series depending on which shard it is coming from. In rare cases
it’s actually uninteresting on which shard something happened so we might always
unconditionally aggregate the shard label away.

In the majority of cases we determine the aggregation labels dynamically based
on the aggregate_metrics config flag. If turned off we usually don’t aggregate
any labels and expose the full set of labels while if it’s turned on we usually
aggregate a subset of labels away. Common ones are the shard, partition or topic
label. Basically any label that can result in high cardinality.

Unless you have a good reason not to you’ll generally want to aggregate any
label that can scale unbounded (with for example partitions or topics). As of
today we also always aggregate the shard label in this case. Though this is
something we might want to change in the future.

Metrics aggregation is on by default in Redpanda Cloud. As of 23.3+ the config flag can
be changed dynamically at runtime without the need for a restart. Hence it’s
easy to get access to all the labels when needed. 

Note that when talking about metrics overhead we mostly talk about downstream
systems of redpanda (i.e.: prometheus itself, grafana etc.) metrics collection
itself has a measurable overhead on redpanda itself. Metrics aggregation
amplifies this as it kind of trades CPU in redpanda for less IO/CPU further
downstream. Hence metrics aggregation is not a wildcard to adding a massive
amount of labels that are later being aggregated away. 

## Adding new metrics

### Counters / Gauges

Adding counters and gauges is generally always fine as those just add a single
new series per shard. Having a static set of extra labels is also fine (e.g.:
the handler type of the kafka API) as those can’t grow unbounded.

Think twice whether labels that can scale dynamically are really needed, i.e.:
topic/partition labels. Always aggregate those when aggregation is turned on.

### Histograms

More care needs to be taken when adding histograms. A single histogram adds more
than 26 metrics series on internal metrics. Hence a histogram has more than 10x
the overhead of a counter or gauge.

This means that any additional label should be considered carefully. Adding
dynamic labels such as partition or topic labels should be avoided altogether.
Keep static labels such as the handler type for the core/really important
histograms. In that example consider whether a separate histogram for
produce/consume/all-other-handlers would be enough. 

### Topics vs. partitions

As mentioned previously the two label types that really cause problems are
partition and topic labels. These can scale dynamically/unbounded and massively
increase the metrics cardinality.

Note that one might think that the topic label should add a lot less series than
the partition label. While this is true most of the time it is unfortunately not
always the case. Some customers might create many many topics (think topic per
“client” or something) with relatively little partitions.

Hence you should think of topic having the same cardinality as partition. 

## Public metrics

In Redpanda we have two different metric endpoints /metrics and /public_metrics.
The former is sometimes referred to as the "internal" endpoint. In Redpanda
Cloud only the public endpoint is exposed.

Public metrics are intended to be low-volume and high-value. They should provide
essential observability without overwhelming downstream systems. The public
endpoint is typically up to 100x smaller than the internal one.

Because of this requirement the rules are more strict in terms of how many
metrics series to add. Aggregation is always enabled on the public endpoint
regardless of the aggregate_metrics config flag.

When adding metrics to the public endpoint keep these in mind:

- Aggregate most labels to avoid any form of scaling with shard, partition, etc.

- Consider whether the metric fits the public metrics criteria. For example, an
  internal buffer ratio might be helpful for debugging but does not qualify as a
  public metric. Most metrics fall into this category and belong only on the
  internal endpoint.

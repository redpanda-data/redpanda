# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.redpanda import MetricsEndpoint
from rptest.services.redpanda import FAILURE_INJECTION_LOG_ALLOW_LIST
from rptest.services.storage_failure_injection import FailureInjectionConfig, \
    NTPFailureInjectionConfig, FailureConfig, NTP, Operation, BatchType
from rptest.utils.si_utils import BucketView


def _generate_failure_injection_config(
        topic_names,
        partitions_per_topic,
        batchtypes=None) -> FailureInjectionConfig:
    """
    Prepare all needed configuration for the failures
    """
    # values greater than this provide unnesessary heavy impact
    failure_probability = 0.0001
    delay_probability = 10
    min_delay_ms = 10
    max_delay_ms = 30

    # by default we target archival metadata
    if batchtypes is None:
        batchtype = [BatchType.archival_metadata]

    # Focus on targeted failure type
    failure_configs = []
    for batchtype in batchtypes:
        # add configured failure types for all operations
        failure_configs += [
            FailureConfig(operation=op,
                          batch_type=batchtype,
                          delay_probability=delay_probability,
                          min_delay_ms=min_delay_ms,
                          max_delay_ms=max_delay_ms,
                          failure_probability=failure_probability)
            for op in Operation
        ]

    ntps: list[NTP] = []
    ntps.append(NTP(namespace="redpanda", topic="controller", partition=0))

    # Each shard gets its own kvstore ntp. The servers
    # on which the test is running will have less than 64 cores,
    # but having failure configuration for non-existing
    # partitions is not an issue.
    for shard in range(64):
        ntps.append(NTP(namespace="redpanda", topic="kvstore", partition=0))

    for topic in topic_names:
        ntps += [
            NTP(topic=topic, partition=p) for p in range(partitions_per_topic)
        ]

    ntp_failure_configs = [
        NTPFailureInjectionConfig(ntp=ntp, failure_configs=failure_configs)
        for ntp in ntps
    ]

    return FailureInjectionConfig(seed=0,
                                  ntp_failure_configs=ntp_failure_configs)


def _calculate_statistic(topics, rpk, redpanda):
    # TODO: Update for multiple topics used
    _stats = {
        # Sum of all watermarks from every partition
        "hwm": 0,
        # Maximum timestamp among all partitions in a topic
        "local_ts": 0,
        # Maximum timestamp among all partitions in a bucket
        "uploaded_ts": 0,
        # (offset_delta, segment_count, last_term)
        "partition_deltas": [],
    }

    # shortcut for max from list
    def _check_and_set_max(list, key):
        if not list:
            return
        else:
            _m = max(list)
            _stats[key] = _m if _stats[key] < _m else _stats[key]

    bucket = BucketView(redpanda)

    # Calculate high watermark sum for all partitions in topic
    for topic in topics:
        _lts_list = []
        _uts_list = []
        for partition in rpk.describe_topic(topic.name):
            # Add currect high watermark for topic
            _stats["hwm"] += partition.high_watermark

            # get next max timestamp fot current partition
            _lts_list += [
                int(
                    rpk.consume(topic=topic.name,
                                n=1,
                                partition=partition.id,
                                offset=partition.high_watermark - 1,
                                format="%d\\n").strip())
            ]

            # get manifest from bucket
            _m = bucket.manifest_for_ntp(topic.name, partition.id)

            # get latest timestamp from segments in current partiton
            if _m['segments']:
                _uts_list += [
                    max(s['max_timestamp'] for s in _m['segments'].values())
                ]
                _top_segment = list(_m['segments'].values())[-1]
                _stats['partition_deltas'].append([
                    _m['partition'], _top_segment['delta_offset_end'],
                    len(_m['segments']), _top_segment['segment_term']
                ])

        # get latest timestamps
        _check_and_set_max(_lts_list, "local_ts")
        _check_and_set_max(_uts_list, "uploaded_ts")

    # Uploads
    # Check manifest upload metrics:
    #  - we should not have uploaded the manifest more times
    #    then there were manifest upload intervals in the runtime.
    _stats["manifest_uploads"] = redpanda.metric_sum(
        metric_name="redpanda_cloud_storage_partition_manifest_uploads_total",
        metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
    _stats["segment_uploads"] = redpanda.metric_sum(
        metric_name="redpanda_cloud_storage_segment_uploads_total",
        metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
    _stats["spillover_manifest_uploads_total"] = redpanda.metric_sum(
        metric_name="redpanda_cloud_storage_spillover_manifest_uploads_total",
        metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)

    return _stats


def _get_timings_table(timings):
    """
    Creates pretty timings table out of dict
    Example dict:
    {
        "timing1": {1: Number, 2: Number},
        "timing2": {2: Number},
        "timing3": {2: Number, 3: Number}
    }
    Table:
                1       2       3
    timing1:    N       N      n/a
    timing2:   n/a      N      n/a
    timing3:   n/a      N       N
    """
    _max_index = max([max(list(v.keys())) for v in timings.values()])

    # Log pretty table of timings
    # first row
    lines = []
    line = " " * 18
    for idx in range(1, _max_index + 1):
        line += f"{idx:^14}"
    lines.append(line)
    # process rows
    for k, v in timings.items():
        line = f"{k:<18}"
        for idx in range(1, _max_index + 1):
            # In case timing not saved, use 'n/a'
            _val = v[idx] if idx in v else "n/a"
            # print use special fmt for seconds
            if k in [
                    # hardcoded for infinite retention tests
                    # TODO: move to separate class with
                    # value units set: s - seconds, etc
                    "high_watermark",
                    "manifest_uploads",
                    "segment_uploads",
                    "consumed_messages",
                    "sp_mn_uploads_total"
            ] or isinstance(_val, str):
                _t = f"{_val}"
            else:
                _t = f"{_val:.2f}s"
            line += f"{_t:^14}"
        lines.append(line)
    return "\n".join(lines)

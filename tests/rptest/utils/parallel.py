# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import concurrent.futures
import itertools


def execute_in_parallel(items, process_batch, batch_len=10):
    """
    Executes a given function in parallel on batches of items.
    Arranges results, if any, into a list in the same order as the input.

    :param items: List of items to process.
    :param process_batch: Function to process a batch of items.
    Expected to return a list of results or None.
    :param batch_len: Number of items per batch.
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        batches = (items[i : i + batch_len] for i in range(0, len(items), batch_len))
        batched_results = list(executor.map(process_batch, batches))
        if all(batch_result is None for batch_result in batched_results):
            return None
        return list(itertools.chain(*batched_results))

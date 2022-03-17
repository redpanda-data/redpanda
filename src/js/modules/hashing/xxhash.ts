/**
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

import { XXHash64 } from "xxhash";

export function RpcXxhash64(data: Buffer) {
  const seed = 0;
  var hash = new XXHash64(seed);
  hash.update(data);
  return hash.digest().readBigUInt64LE(0);
}

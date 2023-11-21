// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate redpanda_transform_sdk as redpanda;

use anyhow::Result;
use redpanda::*;

fn main() {
    on_record_written(my_transform);
}

fn my_transform(event: WriteEvent) -> Result<Vec<Record>> {
    Ok(vec![Record::new_with_headers(
        event.record.key().map(|b| b.to_owned()),
        event.record.value().map(|b| b.to_owned()),
        event
            .record
            .headers()
            .iter()
            .map(|h| h.to_owned())
            .collect(),
    )])
}

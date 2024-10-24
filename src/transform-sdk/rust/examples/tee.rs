// Copyright 2024 Redpanda Data, Inc.
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

use anyhow::Result;
use redpanda_transform_sdk::*;

// This example shows the basic usage of the crate:
// This transform does nothing but copy the same data from an
// input topic to all output topics.
fn main() {
    // Make sure to register your callback and perform other setup in main
    on_record_written(my_transform);
}

// This will be called for each record in the source topic.
//
// The output records returned will be written to all destination topics.
fn my_transform(event: WriteEvent, writer: &mut RecordWriter) -> Result<()> {
    // Redpanda automatically populates environment variables with the output topic configuration.
    // We can dynamically lookup our output topic using these environment variables.
    let output_topics =
        (0..8).filter_map(|i| std::env::var(std::format!("REDPANDA_OUTPUT_TOPIC_{}", i)).ok());
    for topic in output_topics {
        writer.write_with_options(event.record.as_ref(), WriteOptions::to_topic(&topic))?;
    }
    Ok(())
}

# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import string
import uuid
import os


def random_string(N):
    return ''.join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(N))


class WasmScript:
    def __init__(self, inputs=[], outputs=[], script=None):
        self.name = random_string(10)
        self.inputs = inputs
        self.outputs = outputs
        self.script = script
        self.dir_name = str(uuid.uuid4())

    def get_artifact(self, build_dir):
        artifact = os.path.join(build_dir, self.dir_name, "dist",
                                f"{self.dir_name}.js")
        if not os.path.exists(artifact):
            raise Exception(f"Artifact {artifact} was not built")
        return artifact

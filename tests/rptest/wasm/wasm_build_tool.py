# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import tempfile
import shutil
import subprocess
import jinja2


class WasmTemplateRepository:
    IDENTITY_TRANSFORM = "identity_transform.j2"
    FILTER_TRANSFORM = "filter_transform.j2"

    def __init__(self, template_dir):
        self.template_dir = template_dir

    def get(self, script):
        tcp = os.path.join(self.template_dir, script)
        with open(tcp, "r") as fh:
            return fh.read()
        return None


class DirectoryContext():
    def __init__(self, directory):
        self._directory = directory
        self._orig_dir = None

    def __enter__(self):
        self._orig_dir = os.getcwd()
        os.chdir(self._directory)

    def __exit__(self, type, value, traceback):
        os.chdir(self._orig_dir)


class WasmBuildTool():
    def __init__(self, rpk_tool):
        self._rpk_tool = rpk_tool
        self.work_dir = tempfile.mkdtemp()
        dir_path = os.path.dirname(os.path.realpath(__file__))
        self._repo = WasmTemplateRepository(os.path.join(
            dir_path, "templates"))

    def __del__(self):
        shutil.rmtree(self.work_dir)

    def _compile_template(self, script, template):
        inputs = ",".join([
            f'["{topic}", PolicyInjection.Stored]' for topic in script.inputs
        ])
        if any(x is None for x in script.outputs):
            raise Exception('Error rendering template, outputs invalid')
        t = jinja2.Template(template)
        return t.render(input_topics=inputs, output_topics=script.outputs)

    def _build_source(self, artifact_dir):
        with DirectoryContext(artifact_dir) as _:
            subprocess.run(["npm", "install"])
            subprocess.run(["npm", "run", "build"])

    def build_test_artifacts(self, script):
        artifact_dir = os.path.join(self.work_dir, script.dir_name)
        self._rpk_tool.wasm_gen(artifact_dir)
        template = self._repo.get(script.script)
        if template is None:
            raise Exception(f"Template doesn't exist: {script.script}")
        coprocessor = self._compile_template(script, template)
        script_path = os.path.join(artifact_dir, "src",
                                   f"{script.dir_name}.js")
        with open(script_path, "w") as f:
            f.write(coprocessor)
        self._build_source(artifact_dir)

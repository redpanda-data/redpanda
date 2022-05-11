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


class WasmBuildToolException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return f"WasmBuildToolException<{self.msg}>"


class WasmBuildTool():
    def __init__(self, rpk_tool):
        self._rpk_tool = rpk_tool
        self._redpanda = self._rpk_tool._redpanda
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
            raise WasmBuildToolException(
                'Error rendering template, outputs invalid')
        t = jinja2.Template(template)
        return t.render(input_topics=inputs, output_topics=script.outputs)

    def _build_source(self, artifact_dir):
        self._run_npm_cmd(artifact_dir, ["install"])
        self._run_npm_cmd(artifact_dir, ["run", "build"])

    def _run_npm_cmd(self, artifact_dir, cmd):
        with DirectoryContext(artifact_dir) as _:
            retries = 2
            while True:
                output = subprocess.run(['npm'] + cmd,
                                        capture_output=True,
                                        text=True,
                                        check=False)
                if output.returncode != 0:
                    # Retry on network errors, as npm requires external repositories to be available
                    if retries > 0 and "ERR_SOCKET" in output.stderr:
                        self._redpanda.logger.warn(
                            f"Retrying npm command on error: {output.stderr}")
                        retries -= 1
                    else:
                        raise WasmBuildToolException(
                            f"Encountered error building wasm script: {output.stderr}"
                        )
                else:
                    # Success
                    self._redpanda.logger.info(
                        f"npm - stdout: {output.stdout}")
                    self._redpanda.logger.info(
                        f"npm - stderr: {output.stderr}")
                    break

    def build_test_artifacts(self, script):
        artifact_dir = os.path.join(self.work_dir, script.dir_name)
        self._rpk_tool.wasm_gen(artifact_dir)
        template = self._repo.get(script.script)
        if template is None:
            raise WasmBuildToolException(
                f"Template doesn't exist: {script.script}")
        coprocessor = self._compile_template(script, template)
        script_path = os.path.join(artifact_dir, "src",
                                   f"{script.dir_name}.js")
        with open(script_path, "w") as f:
            f.write(coprocessor)
        self._build_source(artifact_dir)

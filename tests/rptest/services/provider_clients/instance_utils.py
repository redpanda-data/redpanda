import os
import json
from typing import Optional

# default specs are a fail safe proof variation of minimal VM specs possible
instance_specs = {
    "default": {
        "vcpus": 2,
        "ram": 16
    },
    "i3en.xlarge": {
        "vcpus": 4,
        "ram": 32
    },
    "i3en.2xlarge": {
        "vcpus": 8,
        "ram": 64
    },
    "i3en.3xlarge": {
        "vcpus": 12,
        "ram": 96
    },
    "i3en.6xlarge": {
        "vcpus": 24,
        "ram": 192
    },
    "i3en.12xlarge": {
        "vcpus": 48,
        "ram": 384
    },
    "i3en.24xlarge": {
        "vcpus": 96,
        "ram": 768
    },
    "is4gen.large": {
        "vcpus": 2,
        "ram": 12
    },
    "is4gen.xlarge": {
        "vcpus": 4,
        "ram": 24
    },
    "is4gen.2xlarge": {
        "vcpus": 8,
        "ram": 48
    },
    "is4gen.4xlarge": {
        "vcpus": 16,
        "ram": 96
    },
    "is4gen.8xlarge": {
        "vcpus": 32,
        "ram": 192
    },
    "n2-highmem-4": {
        "vcpus": 4,
        "ram": 32
    },
    "n2-highmem-8": {
        "vcpus": 8,
        "ram": 64
    },
    "n2-highmem-16": {
        "vcpus": 16,
        "ram": 128
    },
    "n2-highmem-32": {
        "vcpus": 32,
        "ram": 256
    }
}


class ProviderInstanceUtils():
    def __init__(self, logger) -> None:
        self.logger = logger

    def get_metadata_for_node(self, provider, node) -> dict:
        # get path to client_utils.py
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "client_utils.py")

        # Create path to target
        target_path = os.path.join("/tmp", "get_meta.py")

        # copy script to target node
        node.account.copy_to(script_path, target_path)

        # Run
        provider = provider.upper()
        _out = node.account.ssh_output(f"python3 {target_path} {provider}")

        # Cleanup
        node.account.ssh_output(f"rm {target_path}")

        return json.loads(_out)

    def _detect_provider(self, context) -> Optional[str]:
        _provider = None
        if 'cloud_provider' not in context.globals.keys():
            self.logger.info("Can't detect cloud provider, using 'default'")
            self.logger.debug("'cloud_provider not found in globals.json. "
                              "Keys available: "
                              f"{(', '.join(context.globals.keys()))}")
        else:
            _provider = context.globals['cloud_provider'].upper()

        return _provider

    def _get_instance_type(self, provider, metadata) -> str:
        # Use default for any provider
        instance_type = "default"
        if provider == 'AWS':
            if 'instance-type' in metadata:
                instance_type = metadata['instance-type']
            else:
                return "default"
        elif provider == 'GCP':
            if 'machine-type' in metadata:
                # Cut the type name from the variable
                # Example content: 'projects/606234194099/machineTypes/n2-highmem-4'
                instance_type = metadata['machine-type'].split('/')[-1]
            else:
                return "default"
        else:
            raise RuntimeError(f"Unsupported provider: '{provider}'")
        return instance_type

    def get_node_specs(self, node, context) -> dict:
        # Get provider
        provider = self._detect_provider(context)
        if not provider:
            # No provider, exit right away with default specs
            return instance_specs["default"]
        else:
            provider = provider.upper()

        # Getting instance metadata
        self.logger.info("Queriyng metadata for flink instance "
                         f"'{node.account.hostname}'")
        _meta = self.get_metadata_for_node(provider, node)
        # handle errors
        # Occasionally, metadata returned as
        # b'{":%20401,%20\'Unauthorized\'": ""}'
        if len(_meta.keys()) < 2:
            self.logger.debug("Failed to get metadata. Using 'default' specs")
            return instance_specs["default"]
        # Get instance type from meta
        instance_type = self._get_instance_type(provider, _meta)
        # If available, get specs
        if instance_type in instance_specs:
            self.logger.info(f"Using specs for '{instance_type}'")
            return instance_specs[instance_type]
        else:
            self.logger.info(
                f"No record found for '{instance_type}', using 'default'")
            return instance_specs["default"]

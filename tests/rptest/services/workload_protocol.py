# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from abc import abstractmethod
from typing import Protocol, Optional, ClassVar, Any

from rptest.services.redpanda_installer import RedpandaInstaller, RedpandaVersion, RedpandaVersionLine, RedpandaVersionTriple
from rptest.tests.redpanda_test import RedpandaTest


class PWorkload(Protocol):
    DONE: ClassVar[int] = -1
    NOT_DONE: ClassVar[int] = 1
    """
    member variable to access RedpandaTest facilities
    """
    ctx: RedpandaTest

    def get_workload_name(self) -> str:
        return self.__class__.__name__

    def get_earliest_applicable_release(
            self) -> Optional[RedpandaVersionLine | RedpandaVersionTriple]:
        """
        returns the earliest release that this Workload can operate on.
        None -> use the oldest available release
        (X, Y) -> use the latest minor version in line vX.Y
        (X, Y, Z) -> use the release vX.Y.Z
        """
        return None

    def get_latest_applicable_release(self) -> RedpandaVersion:
        """
        returns the latest release that this Workload can operate on.
        RedpandaInstaller.HEAD -> use head version (compiled from source
        (X, Y) -> use the latest minor version in line vX.Y
        (X, Y, Z) -> use the release vX.Y.Z
        """
        return RedpandaInstaller.HEAD

    def begin(self) -> None:
        """
        This method is called before starting the workload. the active redpanda version is self->get_earliest_applicable_relase().
        use this method to set up the topic this workload will operate on, with a unique and descriptive name.
        Additionally, this method should setup an external service that will produce and consume data from the topic.
        """
        return

    def on_partial_cluster_upgrade(
            self, versions: dict[Any, RedpandaVersionTriple]) -> int:
        """
        This method is called while upgrading a cluster, in a mixed state where some of the nodes will have the new version and some the old one.
        versions is a dictionary of redpanda node->version 
        """
        return PWorkload.DONE

    @abstractmethod
    def on_cluster_upgraded(self, version: RedpandaVersionTriple) -> int:
        """
        This method is called to ensure that Workload is progressing on the active redpanda version
        use this method to check the external services and the Workload invariants on the active redpanda version.
        return self.DONE to signal that no further check is needed for this redpanda version
        return self.NOT_DONE to signal that self.progress should be called again on this redpanda version
        """
        raise NotImplementedError

    def end(self) -> None:
        """
        This method is called after the last call of progress, the repdanda active version is self.get_latest_applicable_release().
        use this method to tear down external services and perform cleanup.
        """
        return

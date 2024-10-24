import os
import sys
from copy import deepcopy

# Init path to workloads subfolder
workloads_folder = os.path.split(__file__)[0]
workloads_folder = os.path.join(workloads_folder, "workloads")
# template for single workload in the list
workload_item = {"name": "", "type": "", "path": "", "tags": []}


class WorkloadManager:
    """
        Class manages workloads for e2e tests and provides path to workload
        and able to give list of workload according to given tags.
    """
    # Extension filters
    supported_extensions = ['py', 'jar']
    ignored_extensions = ['json', 'yaml', 'cfg']

    def __init__(self, logger) -> None:
        self.logger = logger
        self.workloads = self._search_workloads(workloads_folder)

    def _search_workloads(self, path):
        """
            Searched workloads in subfolder and collects info on them
        """
        str_extensions = ", ".join(self.supported_extensions)
        cfg_extensions = ", ".join(self.ignored_extensions)
        self.logger.info(f"Loading workloads from '{path}' "
                         f"with extensions of {str_extensions}. "
                         f"Ignored files are {cfg_extensions}")
        workloads = []
        # Get all workload files
        files = os.listdir(path)
        for item in files:
            item_path = os.path.join(path, item)
            # If not a file, skip
            if not os.path.isfile(item_path):
                # Do not process subfolders at this time
                self.logger.debug(f"'{item_path}' not a file, skipped")
                # TODO: Add subfolders as tags with a recoursion
                continue
            # if name ends with 'py' or 'jar' inside workloads folder
            # just add it to processing
            filename = item.split('.')[0]
            ext = item.split('.')[1]
            if ext in self.ignored_extensions:
                # Silent skip for ignored files
                self.logger.info(f"Ignoring configuration file: '{item_path}'")
                continue
            elif ext not in self.supported_extensions:
                # Log file that is definitely not a workload
                self.logger.warning(
                    f"Ignoring unsupported file: '{item_path}'")
                continue
            else:
                # Load this
                new_workload = deepcopy(workload_item)
                new_workload['name'] = filename
                new_workload['type'] = ext
                new_workload['path'] = item_path
                new_workload['tags'] = filename.split('_')

                workloads.append(new_workload)

        # Iterate py workloads and collect tags
        return workloads

    def get_workloads(self, tag_list):
        """
            Provides workloads according to given tags
        """
        workload_list = []

        # Iterate workloads and filter them by intersect their tag list
        # with provided tag_list
        for workload in self.workloads:
            tag_intersect = set(workload['tags']).intersection(tag_list)
            if len(tag_intersect) == len(tag_list):
                workload_list.append(workload)
        return workload_list

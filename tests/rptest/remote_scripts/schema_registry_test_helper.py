# This is not a test.  It is a remote script for use by schema_registry_test.py

import threading
import requests
import sys
import logging
import random
import time
import json

log = logging.getLogger("helper")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

# How many errors before a worker gives up?
max_errs = 1

HTTP_GET_HEADERS = {"Accept": "application/vnd.schemaregistry.v1+json"}

HTTP_POST_HEADERS = {
    "Accept": "application/vnd.schemaregistry.v1+json",
    "Content-Type": "application/vnd.schemaregistry.v1+json"
}

schema_template = '{"type":"record","name":"record_%s","fields":[{"name":"f1","type":"string"}]}'


class WriteWorker(threading.Thread):
    def __init__(self, name, count, node_names):
        super(WriteWorker, self).__init__()
        self.daemon = True
        self.name = name
        self.count = count
        self.schema_counter = 1
        self.results = {}
        self.nodes = node_names

        self.errors = []

    def _request(self, verb, path, hostname=None, **kwargs):
        """

        :param verb: String, as for first arg to requests.request
        :param path: URI path without leading slash
        :param timeout: Optional requests timeout in seconds
        :return:
        """

        if hostname is None:
            # Pick hostname once: we will retry the same place we got an error,
            # to avoid silently skipping hosts that are persistently broken
            nodes = [n for n in self.nodes]
            random.shuffle(nodes)
            hostname = nodes[0]

        uri = f"http://{hostname}:8081/{path}"

        if 'timeout' not in kwargs:
            kwargs['timeout'] = 60

        # Error codes that may appear during normal API operation, do not
        # indicate an issue with the service
        acceptable_errors = {409, 422, 404}

        def accept_response(resp):
            return 200 <= resp.status_code < 300 or resp.status_code in acceptable_errors

        log.debug(f"{verb} hostname={hostname} {path} {kwargs}")

        # This is not a retry loop: you get *one* retry to handle issues
        # during startup, after that a failure is a failure.
        r = requests.request(verb, uri, **kwargs)
        if not accept_response(r):
            log.info(
                f"Retrying for error {r.status_code} on {verb} {path} ({r.text})"
            )
            time.sleep(10)
            r = requests.request(verb, uri, **kwargs)
            if accept_response(r):
                log.info(
                    f"OK after retry {r.status_code} on {verb} {path} ({r.text})"
                )
            else:
                log.info(
                    f"Error after retry {r.status_code} on {verb} {path} ({r.text})"
                )

        log.info(f"{r.status_code} {verb} hostname={hostname} {path} {kwargs}")

        return r

    def _post_subjects_subject_versions(self,
                                        subject,
                                        data,
                                        headers=HTTP_POST_HEADERS,
                                        **kwargs):
        return self._request("POST",
                             f"subjects/{subject}/versions",
                             headers=headers,
                             data=data,
                             **kwargs)

    def _get_subjects_subject_versions_version(self,
                                               subject,
                                               version,
                                               headers=HTTP_GET_HEADERS):
        return self._request("GET",
                             f"subjects/{subject}/versions/{version}",
                             headers=headers)

    def _get_schemas_ids_id(self, id, headers=HTTP_GET_HEADERS):
        return self._request("GET", f"schemas/ids/{id}", headers=headers)

    def create(self):
        for i in range(0, self.count):
            subject = f"subject_{self.name}_{i}"
            version_id = 1
            schema_def = schema_template % f"{self.name}_{i}"

            log.debug(f"Worker {self.name} writing subject {subject}")
            try:
                resp = self._post_subjects_subject_versions(
                    subject=subject,
                    data=json.dumps({"schema": schema_def}),
                    timeout=20)
            except requests.exceptions.RequestException as e:
                self._push_err(f"{subject}/{version_id} POST exception: {e}")
            else:
                self._check_eq(subject, version_id, "post_ret",
                               resp.status_code, 200)
                if resp.status_code == 200:
                    schema_id = resp.json()["id"]
                    self.results[(subject, version_id)] = (schema_def,
                                                           schema_id)

    def _push_err(self, err):
        log.error(f"push_err[{self.name}]: {err}")
        self.errors.append(err)
        if len(self.errors) > max_errs:
            # On e.g. timeouts, test takes too long if we wait for every request to time out
            raise RuntimeError("Too many errors on worker {self.name}")

    def _check_eq(self, subject, version, check_name, a, b):
        if a != b:
            self._push_err(f"{subject}/{version} failed {check_name} {a}!={b}")

    def verify(self):
        for (subject, version), (schema_def,
                                 schema_id) in self.results.items():
            try:
                r = self._get_subjects_subject_versions_version(
                    subject, version)
            except requests.exceptions.RequestException as e:
                self._push_err(
                    f"{subject}/{version} GET version exception: {e}")
                continue

            self._check_eq(subject, version, "subject_retcode", r.status_code,
                           200)
            self._check_eq(subject, version, "subject",
                           r.json()['subject'], subject)
            self._check_eq(subject, version, "version",
                           r.json()['version'], version)
            self._check_eq(subject, version, "schema",
                           r.json()['schema'], schema_def)

            try:
                r = self._get_schemas_ids_id(id=schema_id)
            except requests.exceptions.RequestException as e:
                self._push_err(
                    f"{subject}/{version} GET schema exception: {e}")
                continue
            self._check_eq(subject, version, "schema_retcode", r.status_code,
                           200)
            self._check_eq(subject, version, "schema_def",
                           r.json()['schema'], schema_def)

        schema_ids = self.get_schema_ids()
        if len(set(schema_ids)) != len(schema_ids):
            self._push_err(f"Schema IDs reused!")

    def run(self):
        try:
            t1 = time.time()
            self.create()
            create_dur = time.time() - t1
            log.info(
                f"[{self.name}] Created {self.count} in {create_dur}s ({self.count / create_dur}/s)"
            )

            # Drop out early if we already found some errors during write
            if len(self.errors) == 0:
                self.verify()
        except Exception as e:
            self.errors.append(f"Exception!  {e}")
            raise

    def get_schema_ids(self):
        result = []
        for (subject, version), (schema_def,
                                 schema_id) in self.results.items():
            result.append(schema_id)

        return result


def stress_test(node_names):
    # Even this relatively small number of concurrent writers is
    # more stress than the system will encounter in the field: schemas
    # are likely to be written occasionally and not from many clients at once.
    # However, it is enough to push the system to hit its collision/retry path
    # for writes.
    subjects_per_worker = 16
    n_workers = 4
    workers = []
    for n in range(0, n_workers):
        worker = WriteWorker(f"{n}", subjects_per_worker, node_names)
        workers.append(worker)

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()

    all_schema_ids = []
    for worker in workers:
        if worker.errors:
            log.error(f"Worker {worker.name} errors:")
            for e in worker.errors:
                log.error(f"  Worker {worker.name}: {e}")
        else:
            log.info(f"Worker {worker.name} OK")

        all_schema_ids.extend(worker.get_schema_ids())

    assert sum([len(worker.errors) for worker in workers]) == 0

    # Check no schema IDs were duplicated
    assert len(all_schema_ids) == len(set(all_schema_ids))


if __name__ == "__main__":
    node_names = sys.argv[1:]

    assert len(node_names) > 1
    stress_test(node_names)

    log.info("All checks passed")

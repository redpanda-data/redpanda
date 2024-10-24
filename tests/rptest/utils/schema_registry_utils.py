# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from enum import Enum
import random
import requests
import time

HTTP_GET_HEADERS = {"Accept": "application/vnd.schemaregistry.v1+json"}

HTTP_POST_HEADERS = {
    "Accept": "application/vnd.schemaregistry.v1+json",
    "Content-Type": "application/vnd.schemaregistry.v1+json"
}


class Mode(str, Enum):
    READWRITE = "READWRITE"
    READONLY = "READONLY"

    def to_json(self) -> dict:
        return {"mode": self.value}


def _request(nodes, logger, verb, path, hostname=None, **kwargs):
    if hostname is None:
        nodes = [n for n in nodes]
        random.shuffle(nodes)
        node = nodes[0]
        hostname = node.account.hostname

    scheme = 'http'
    uri = f'{scheme}://{hostname}:8081/{path}'

    if 'timeout' not in kwargs:
        kwargs['timeout'] = 60

    # Error codes that may appear during normal API operation, do not
    # indicate an issue with the service
    acceptable_errors = {409, 422, 404}

    def accept_response(resp):
        return 200 <= resp.status_code < 300 or resp.status_code in acceptable_errors

    logger.debug(f"{verb} hostname={hostname} {path} {kwargs}")

    r = requests.request(verb, uri, **kwargs)
    if not accept_response(r):
        logger.info(
            f"Retrying for error {r.status_code} on {verb} {path} ({r.text})")
        time.sleep(10)
        r = requests.request(verb, uri, **kwargs)
        if accept_response(r):
            logger.info(
                f"OK after retry {r.status_code} on {verb} {path} ({r.text})")
        else:
            logger.info(
                f"Error after retry {r.status_code} on {verb} {path} ({r.text})"
            )

    logger.info(f"{r.status_code} {verb} hostname={hostname} {path} {kwargs}")

    return r


def get_config(nodes, logger, headers=HTTP_GET_HEADERS, **kwargs):
    """Returns the config of the schema registry
    
    Parameters
    ----------
    nodes : 
        List of nodes to pick from
    
    logger :
        Logger to use

    headers : {str:str}, optional
        Headers to use

    **kwargs : optional
        Additional parameters to request module

    Returns
    -------
    The configuration of SR
    """
    return _request(nodes, logger, "GET", "config", headers=headers, **kwargs)


def get_subjects(nodes,
                 logger,
                 deleted=False,
                 headers=HTTP_GET_HEADERS,
                 **kwargs):
    """Returns a list of subjects held by the SR
    
    Parameters
    ----------
    nodes : 
        List of nodes to pick from
    
    logger :
        Logger to use

    deleted : bool, default=False
       If True, return deleted subjects as well

    headers : {str:str}, optional
        Headers to use

    **kwargs : optional
        Additional parameters to request module

    Returns
    -------
    List of subjects
    """
    return _request(nodes,
                    logger,
                    "GET",
                    f"subjects{'?deleted=true' if deleted else ''}",
                    headers=headers,
                    **kwargs)


def put_mode(nodes, logger, mode: Mode, headers=HTTP_POST_HEADERS, **kwargs):
    """Sets the mode. Requires superuser privileges

    Parameters
    ----------
    nodes :
        List of nodes to pick from

    logger :
        Logger to use

    mode : Mode
       The mode to change to

    headers : {str:str}, optional
        Headers to use

    **kwargs : optional
        Additional parameters to request module

    Returns
    -------
    Changes the read-write-ness mode of the schema registry
    """
    return _request(nodes,
                    logger,
                    "PUT",
                    "mode",
                    json=mode.to_json(),
                    headers=headers,
                    **kwargs)

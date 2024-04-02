#!/usr/bin/env python3
import argparse
from enum import Enum
import json
import logging
import os
import requests
from requests.auth import HTTPBasicAuth
import subprocess
import sys
import tempfile
from typing import Tuple


class NoJiraIssueFound(Exception):
    pass


class MultipleJiraIssuesFound(Exception):
    pass


class Command(Enum):
    CREATE_ISSUE = 1
    UPDATE_COMMENT = 2
    REOPEN_ISSUE = 3
    CLOSE_ISSUE = 4


class JiraHelper():
    _base_url = 'https://redpandadata.atlassian.net'
    _api_version = 2
    _issue_url = f'{_base_url}/rest/api/{_api_version}/issue'
    _project_key = 'CORE'
    _done_transition_id = 41
    _backlog_transition_id = 11

    def __init__(self, command: Command, logger: logging.Logger):
        self.logger: logging.Logger = logger
        self.command: Command = command
        self._check_env()

    @staticmethod
    def _get_auth() -> HTTPBasicAuth:
        return HTTPBasicAuth(username=os.environ['JIRA_USER'],
                             password=os.environ['JIRA_TOKEN'])

    def _check_env_val(self, val: str):
        self.logger.debug(f'Checking environment for {val}')
        try:
            assert os.environ[
                val] is not None, f"Environment variable {val} must be set"
        except KeyError:
            assert False, f"Environment variable {val} must be set"

    def _check_env(self):
        self._check_env_val('JIRA_USER')
        self._check_env_val('JIRA_TOKEN')

        if self.command == Command.CREATE_ISSUE or self.command == Command.REOPEN_ISSUE:
            self._check_env_val('ISSUE_BODY')
            self._check_env_val('ISSUE_URL')
            self._check_env_val('ISSUE_TITLE')
            self._check_env_val('ISSUE_LABELS')
        elif self.command == Command.UPDATE_COMMENT:
            self._check_env_val('ISSUE_URL')
            self._check_env_val('ISSUE_COMMENT')
        elif self.command == Command.CLOSE_ISSUE:
            self._check_env_val('ISSUE_URL')
        else:
            raise NotImplementedError(
                f"Command {self.command} is not implemented")

    def execute(self):
        self.logger.debug(f'Executing command {self.command}')
        if self.command == Command.CREATE_ISSUE:
            self._create_issue()
        elif self.command == Command.UPDATE_COMMENT:
            self._update_comment()
        elif self.command == Command.REOPEN_ISSUE:
            self._reopen_issue()
        elif self.command == Command.CLOSE_ISSUE:
            self._close_issue()
        else:
            raise NotImplementedError(
                f'Command {self.command} not yet implemented')

    def _get_gh_output(self, command) -> str:
        return subprocess.check_output(command.split(' ')).decode()

    def _transition_issue(self, issue_id, transition_id):
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        payload = json.dumps({"transition": {"id": transition_id}})
        self._submit_request(method="POST",
                             endpoint=f'issue/{issue_id}/transitions',
                             headers=headers,
                             parameters=None,
                             data=payload)

    def _close_issue(self):
        try:
            issue_id = self._find_jira_issue_by_gh_issue_url()
            self.logger.debug(f'Closing issue {issue_id}')
            self._transition_issue(issue_id, self._done_transition_id)
            self._add_comment_to_issue(
                issue_id,
                f"Closing JIRA issue from GitHub issue {os.environ['ISSUE_URL']}"
            )

        except NoJiraIssueFound:
            self.logger.debug(
                'Did not find JIRA issue matching URL, nothing to do')

    def _reopen_issue(self):

        try:
            issue_id = self._find_jira_issue_by_gh_issue_url()
            self.logger.debug(f'Found issue {issue_id}')
            self._transition_issue(issue_id, self._backlog_transition_id)
            self._add_comment_to_issue(issue_id, "Reopened issue")
        except NoJiraIssueFound:
            self.logger.debug(
                'Did not find JIRA issue matching URL, creating it')
            issue_id = self._create_issue()
            issue_comments = json.loads(
                self._get_gh_output(
                    f"gh issue view {os.environ['ISSUE_URL']} --json comments")
            )
            self.logger.debug(f'Comments: {issue_comments}')
            for comment in issue_comments['comments']:
                self._add_comment_to_issue(issue_id, comment['body'])

    def _get_issue_type(self, labels) -> str:
        if 'kind/bug' in labels:
            return 'Bug'
        else:
            return 'Task'

    def _create_issue(self):
        try:
            labels = os.environ['ISSUE_LABELS']
            self.logger.debug(f'Labels: {labels}')
        except Exception:
            pass
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        issue_body = os.environ['ISSUE_BODY']
        issue_url = os.environ['ISSUE_URL']
        issue_title = os.environ['ISSUE_TITLE']
        labels = os.environ['ISSUE_LABELS'].split(',')
        issue_type = self._get_issue_type(labels)
        self.logger.debug(f"Creating an issue of type {issue_type}")

        payload = json.dumps({
            "fields": {
                "description": f"{issue_body}",
                "summary": f"{issue_title}",
                "issuetype": {
                    "name": f"{issue_type}"
                },
                "labels": labels,
                "project": {
                    "key": f"{self._project_key}"
                },
                "customfield_10052": f"{issue_url}"
            }
        })
        response = self._submit_request(method="POST",
                                        endpoint="issue",
                                        data=payload,
                                        headers=headers)

        resp_json = json.loads(response.text)
        issue_key = resp_json['key']
        issue_id = resp_json['id']

        message = """
JIRA Issue created from GitHub issue.  Any updates in JIRA will _not_ be pushed back
to the GitHub issue.  New comments from GitHub will sync with JIRA issue, but not
modifications.  Please refer to the External GitHub Link field to get to the GitHub
issue that triggered this issue's creation.
"""

        self._add_comment_to_issue(issue_id, message)

        jira_issue_url = f'{self._base_url}/browse/{issue_key}'
        new_body = issue_body + f"\n\nJIRA Link: [{issue_key}]({jira_issue_url})"
        self.logger.debug(
            f"Updating issue text to include link to {jira_issue_url}")

        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(new_body.encode())
            tf.flush()
            tf.close
            cmd = ["gh", "issue", "edit", issue_url, "--body-file", tf.name]
            self.logger.debug(f'Executing gh command: {cmd}')
            subprocess.run(cmd)

        return issue_id

    def _submit_request(self,
                        method: str,
                        endpoint: str,
                        headers: str,
                        data=None,
                        parameters=None,
                        api_ver: int = 2) -> requests.Response:
        url = f'{self._base_url}/rest/api/{api_ver}/{endpoint}'

        self.logger.debug(
            f'Sending {method} request to {url} {f"containing {data}" if data is not None else "with no data"} {f"with parameters {parameters}" if parameters is not None else ""}'
        )

        resp = requests.request(method=method,
                                url=url,
                                data=data if data is not None else None,
                                headers=headers,
                                params=parameters if not None else None,
                                auth=self._get_auth())
        self.logger.debug(f'Response: {resp.text}')
        return resp

    def _add_comment_to_issue(self, issue_id, comment):
        self.logger.debug(
            f'Updating issue {issue_id} with comment "{comment}"')
        payload = json.dumps({"body": comment})
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        self._submit_request(method="POST",
                             endpoint=f"issue/{issue_id}/comment",
                             headers=headers,
                             parameters=None,
                             data=payload)

    def _update_comment(self):
        issue_id = self._find_jira_issue_by_gh_issue_url()
        comment_body = os.environ['ISSUE_COMMENT']
        self.logger.debug(f'Found issue with ID {issue_id}')
        self._add_comment_to_issue(issue_id, comment_body)

    def _find_jira_issue_by_gh_issue_url(self) -> str:
        issue_url = os.environ['ISSUE_URL']

        query = {
            'jql':
            f'project = {self._project_key} and "External GitHub Issue[URL Field]" = "{issue_url}"',
            'fields': 'summary'
        }

        headers = {'Accept': 'application/json'}

        resp = json.loads(
            self._submit_request(method="GET",
                                 endpoint="search",
                                 data=None,
                                 parameters=query,
                                 headers=headers).text)
        total_issues = resp["total"]
        if total_issues == 0:
            raise NoJiraIssueFound()
        elif total_issues > 1:
            raise MultipleJiraIssuesFound()

        return resp["issues"][0]["id"]


def parse_args() -> Tuple[Command, bool]:
    parser = argparse.ArgumentParser(
        prog='jira-helper', description='JIRA Github Integration script')
    parser.add_argument('command', choices=[e.name for e in Command])
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        help='Increase verbosity')
    args = parser.parse_args()
    return (Command[args.command], args.verbose)


def main():
    logger = logging.getLogger(__name__)
    (command, verbose) = parse_args()
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    logger.info(f'Executing command {command}')
    helper = JiraHelper(command, logger)
    helper.execute()


if __name__ == "__main__":
    sys.exit(main())

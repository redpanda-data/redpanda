#!/usr/bin/env python3
from atlassian import Jira
import argparse
from enum import Enum
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from typing import Optional, Tuple


class NoJiraIssueFound(Exception):
    pass


class MultipleJiraIssuesFound(Exception):
    pass


class Command(Enum):
    ISSUE = 1
    UPDATE_COMMENT = 2


class IssueState(Enum):
    open = 1
    closed = 2


def get_issue_state(state: str) -> IssueState:
    return IssueState[state.lower()]


class JiraHelper():
    _base_url = 'https://redpandadata.atlassian.net'
    _done_status_name = 'DONE'
    _backlog_status_name = 'BACKLOGGED'
    _bug_labels = ['kind/bug', 'ci-failure']

    def __init__(self, command: Command, logger: logging.Logger, project: str,
                 pandoc: Optional[str]):
        self.logger: logging.Logger = logger
        self.command: Command = command
        self._pandoc = pandoc
        self._project_key = project
        self._check_env()
        self._jira = Jira(url=self._base_url,
                          username=os.environ['JIRA_USER'],
                          password=os.environ['JIRA_TOKEN'],
                          cloud=True)

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

        if self.command == Command.ISSUE:
            self._check_env_val('ISSUE_BODY')
            self._check_env_val('ISSUE_URL')
            self._check_env_val('ISSUE_TITLE')
            self._check_env_val('ISSUE_LABELS')
            self._check_env_val('ISSUE_STATE')
            self._check_env_val('EVENT_NAME')
        elif self.command == Command.UPDATE_COMMENT:
            self._check_env_val('ISSUE_URL')
            self._check_env_val('ISSUE_COMMENT')
        else:
            raise NotImplementedError(
                f"Command {self.command} is not implemented")

    def _get_gh_issue_comments(self, url):
        return json.loads(
            self._run_cmd_return_stdout(
                f'gh issue view {url} --json comments'))

    def _ghm_to_jira(self, ghm: str) -> str:
        if self._pandoc is not None:
            with tempfile.NamedTemporaryFile(delete=False) as tf:
                tf.write(ghm.encode())
                tf.flush()
                tf.close()
                jmd = self._run_cmd_return_stdout(
                    f'{self._pandoc} -f gfm -w jira {tf.name}')
                os.unlink(tf.name)
                return jmd
        return ghm

    def _issue_helper(self):
        event_name = os.environ['EVENT_NAME']

        if event_name == "opened":
            issue_id = self._find_issue()
            if issue_id is not None:
                self.logger.warning(
                    f"Issue {issue_id} already exists for {os.environ['ISSUE_URL']}"
                )
            else:
                self.logger.debug(
                    f"Creating issue for {os.environ['ISSUE_URL']}")
                self._create_issue_and_add_comments()
        elif event_name == "reopened":
            issue_id = self._find_issue()
            self._create_issue_and_add_comments(
            ) if issue_id is None else self._reopen_issue(issue_id)
        elif event_name == 'closed' or event_name == 'deleted':
            issue_id = self._find_issue()
            self._close_issue(issue_id) if issue_id is not None else None
        elif event_name == 'edited':
            issue_id = self._find_issue()
            self._create_issue_and_add_comments(
            ) if issue_id is None else self._update_issue(issue_id)
        elif (event_name == 'labeled' or event_name == 'unlabeled'):
            issue_id = self._find_issue()
            self._update_issue(issue_id) if issue_id is not None else None
        else:
            self.logger.info(f'No action performed for event {event_name}')

    def execute(self):
        self.logger.debug(f'Executing command {self.command}')
        if self.command == Command.ISSUE:
            self._issue_helper()
        elif self.command == Command.UPDATE_COMMENT:
            self._update_comment()
        else:
            raise NotImplementedError(
                f'Command {self.command} not yet implemented')

    def _run_cmd_return_stdout(self, command) -> str:
        return subprocess.check_output(command.split(' ')).decode()

    def _transition_issue(self, issue_id, status_name):
        try:
            self._jira.issue_transition(issue_key=issue_id, status=status_name)
        except Exception as e:
            if "Issue does not exist" in str(e):
                raise NoJiraIssueFound()
            else:
                raise e

    def _close_issue(self, issue_id):
        self._transition_issue(issue_id, self._done_status_name)
        self._add_comment_to_issue(
            issue_id,
            f"Closing JIRA issue from GitHub issue {os.environ['ISSUE_URL']}")

    def _find_issue(self):
        try:
            return self._find_jira_issue_by_gh_issue_url()
        except NoJiraIssueFound:
            return None

    def _create_issue_and_add_comments(self):
        issue_id = self._create_issue()
        self.logger.debug(f'Created issue {issue_id}, now adding comments')
        self._add_comments_to_issue(issue_id)
        return issue_id

    def _add_comments_to_issue(self, issue_id):
        issue_comments = self._get_gh_issue_comments(os.environ['ISSUE_URL'])
        self.logger.debug(f'Comments: {issue_comments}')
        for comment in issue_comments['comments']:
            self._add_comment_to_issue(issue_id, comment['body'])

    def _reopen_issue(self, issue_id):
        self._transition_issue(issue_id, self._backlog_status_name)
        self._add_comment_to_issue(
            issue_id,
            f"Reopening JIRA issue from Github issue {os.environ['ISSUE_URL']}"
        )

    def _bug_label(self, labels) -> bool:
        return any(l in labels for l in self._bug_labels)

    def _get_issue_type(self, labels) -> str:
        if self._bug_label(labels):
            return 'Bug'
        else:
            return 'Task'

    def _get_gh_issue_labels(self):
        try:
            labels = os.environ['ISSUE_LABELS']
            self.logger.debug(f'Labels: {labels}')
            return [l.replace(' ', '-') for l in labels.split(',')]
        except Exception:
            return []

    def _create_issue(self):
        issue_body = os.environ['ISSUE_BODY']
        issue_url = os.environ['ISSUE_URL']
        issue_title = os.environ['ISSUE_TITLE']
        labels = self._get_gh_issue_labels()
        issue_type = self._get_issue_type(labels)

        jira_issue_body = self._ghm_to_jira(issue_body)

        fields = {
            "description" if issue_type == 'Task' else "customfield_10083":
            f"{jira_issue_body}",
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
        self.logger.debug(
            f"Creating an issue of type {issue_type} with fields {fields}")
        new_issue = self._jira.issue_create(fields=fields)
        issue_key = new_issue['key']
        issue_id = new_issue['id']

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

    def _add_comment_to_issue(self, issue_id, comment):
        comment = self._ghm_to_jira(comment)
        self.logger.debug(
            f'Updating issue {issue_id} with comment "{comment}"')
        try:
            self._jira.issue_add_comment(issue_key=issue_id, comment=comment)
        except Exception as e:
            if "Issue does not exist" in str(e):
                raise NoJiraIssueFound()
            else:
                raise e

    def _update_comment(self):
        try:
            issue_id = self._find_jira_issue_by_gh_issue_url()
            comment_body = os.environ['ISSUE_COMMENT']
            self.logger.debug(f'Found issue with ID {issue_id}')
            self._add_comment_to_issue(issue_id, comment_body)
        except NoJiraIssueFound:
            self.logger.warning(
                f"No issue found for {os.environ['ISSUE_URL']}")
        except MultipleJiraIssuesFound:
            self.logger.warning(
                f"Multiple issues found for {os.environ['ISSUE_URL']}")

    def _update_issue(self, issue_id):
        gh_issue_title = os.environ['ISSUE_TITLE']
        gh_issue_labels = self._get_gh_issue_labels()
        gh_issue_labels.sort()
        self.logger.debug(f'GH Labels: {gh_issue_labels}')

        fields = {}
        jira_issue_summary = self._jira.issue_field_value(issue_id,
                                                          field='summary')
        if jira_issue_summary != gh_issue_title:
            self.logger.debug(
                f'Jira issue {issue_id} has a different title: "{jira_issue_summary}" != "{gh_issue_title}")'
            )
            fields['summary'] = gh_issue_title

        jira_issue_labels = self._jira.issue_field_value(issue_id,
                                                         field='labels')
        jira_issue_labels.sort()
        if jira_issue_labels != gh_issue_labels:
            self.logger.debug(
                f'Jira issue {issue_id} labels do not match GH issue: "{jira_issue_labels}" != "{gh_issue_labels}"'
            )
            fields['labels'] = gh_issue_labels

        if len(fields) == 0:
            self.logger.debug('No updates necessary')
        else:
            self.logger.debug(f'Submitting update: {fields}')
            self._jira.update_issue_field(issue_id, fields, notify_users=True)

    def _find_jira_issue_by_gh_issue_url(self) -> str:
        issue_url = os.environ['ISSUE_URL']
        jql_request = f'project = {self._project_key} and "External GitHub Issue[URL Field]" = "{issue_url}"'
        issues = self._jira.jql(jql=jql_request, fields='summary')
        issues = issues['issues']
        if len(issues) == 0:
            raise NoJiraIssueFound()
        elif len(issues) > 1:
            raise MultipleJiraIssuesFound()
        else:
            return issues[0]["id"]


def parse_args() -> Tuple[Command, bool]:
    parser = argparse.ArgumentParser(
        prog='jira-helper', description='JIRA Github Integration script')
    parser.add_argument('command', choices=[e.name for e in Command])
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        help='Increase verbosity')
    parser.add_argument('-p',
                        '--project',
                        required=True,
                        help="The project to use")
    args = parser.parse_args()
    return (Command[args.command], args.verbose, args.project)


def find_program(prog) -> Optional[str]:
    return shutil.which(prog)


def main():
    logger = logging.getLogger(__name__)
    (command, verbose, project) = parse_args()
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    logger.info(f'Executing command {command}')
    helper = JiraHelper(command, logger, project, find_program('pandoc'))
    helper.execute()


if __name__ == "__main__":
    sys.exit(main())

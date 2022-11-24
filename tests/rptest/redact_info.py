#!/usr/bin/python3

import json
import sys


class Redactor:
    """
    Redact the sensitive information like aws account info from the
    ducktape results.
    """
    def __init__(self, ducktape_report_json_file) -> None:
        self.ducktape_report_json_file = ducktape_report_json_file

    def redact_sensitive_info(self):

        with open(self.ducktape_report_json_file) as f:
            ducktape_report_json = json.load(f)

        if 'session_context' in ducktape_report_json:
            if '_globals' in ducktape_report_json['session_context']:
                if 's3_access_key' in ducktape_report_json['session_context'][
                        '_globals']:
                    ducktape_report_json['session_context']['_globals'][
                        's3_access_key'] = '***'
                if 's3_bucket' in ducktape_report_json['session_context'][
                        '_globals']:
                    ducktape_report_json['session_context']['_globals'][
                        's3_bucket'] = '***'
                if 's3_region' in ducktape_report_json['session_context'][
                        '_globals']:
                    ducktape_report_json['session_context']['_globals'][
                        's3_region'] = '***'
                if 's3_secret_key' in ducktape_report_json['session_context'][
                        '_globals']:
                    ducktape_report_json['session_context']['_globals'][
                        's3_secret_key'] = '***'

        with open(self.ducktape_report_json_file, 'w') as f:
            json.dump(ducktape_report_json, f, indent=2, sort_keys=True)


if len(sys.argv) > 1:
    Redactor(sys.argv[1]).redact_sensitive_info()
else:
    Redactor("report.json").redact_sensitive_info()

#!/usr/bin/env python3
import sys
import os
import stat
import logging
import argparse
import getpass
from string import Template

sys.path.append(os.path.dirname(__file__))
logger = logging.getLogger('rp')

import cli
import shell
import git
import log
import fs

HOME = os.getenv("HOME")
DEFAULT_PERMS=stat.S_IREAD | stat.S_IWRITE

class DotFile():
    def template(self):
        pass

    def directory(self):
        pass

    def file_name(self):
        pass

    def permissions(self):
        return DEFAULT_PERMS

    def post_process_hook(self):
        pass


class SystemdGetmailService(DotFile):
    def directory(self):
        tpl = Template("$home/.config/systemd/user")
        return tpl.substitute(home=HOME)

    def file_name(self):
        return "vectorized_mail.service"

    def permissions(self):
        return DEFAULT_PERMS | stat.S_IRGRP

    def post_process_hook(self):
        shell.run_subprocess("systemctl --user enable vectorized_mail")
        shell.run_subprocess("systemctl --user restart vectorized_mail")

    def template(self):
        tpl = Template("""
[Unit]
Description=Run getmail for $email
After=network.target
Wants=network.target

[Service]
Type=oneshot
ExecStart=/bin/getmail --rcfile=$home/.getmail/getmailrc

[Install]
WantedBy=default.target
        """)
        return tpl.substitute(email=git.get_git_email(), home=HOME)


class SystemdGetmailTimer(DotFile):
    def directory(self):
        tpl = Template("$home/.config/systemd/user")
        return tpl.substitute(home=HOME)

    def file_name(self):
        return "vectorized_mail.timer"

    def permissions(self):
        return DEFAULT_PERMS | stat.S_IRGRP

    def post_process_hook(self):
        shell.run_subprocess("systemctl --user enable vectorized_mail.timer")
        shell.run_subprocess("systemctl --user restart vectorized_mail.timer")

    def template(self):
        tpl = Template("""
[Unit]
Description=Run getmail ($email) every 5 minutes

[Timer]
OnActiveSec=5min
OnUnitActiveSec=5min

[Install]
WantedBy=timers.target
        """)
        return tpl.substitute(email=git.get_git_email())


class GetmailRC(DotFile):
    def __init__(self, password):
        self.password = password

    def directory(self):
        return "%s/.getmail" % HOME

    def file_name(self):
        return "getmailrc"

    def template(self):
        tpl = Template("""
[retriever]
type = SimpleIMAPSSLRetriever
server = imap.gmail.com
mailboxes = ("[Gmail]/All Mail", )
username = $email
password = $password

[options]
# only download *new* emails
read_all = false
# do not alter status of emails on server
delivered_to = false
# ditto
received = false
# do not delete emails on server
delete = false
# max downloaded each time; use 0 for no limit
max_messages_per_session = 0
message_log = $home/.getmail/log

[destination]
type = MDA_external
path = /bin/procmail
arguments = ("-f", "%(sender)", "-m", "$home/.procmailrc")
    """)
        return tpl.substitute(email=git.get_git_email(),
                              password=self.password,
                              home=HOME)


class ProcmailRC(DotFile):
    def directory(self):
        return HOME

    def file_name(self):
        return ".procmailrc"

    def template(self):
        return """
PATH=$HOME/bin:/bin:/usr/bin:/usr/local/bin
MAILDIR=$HOME/.maildir
DEFAULT=$HOME/.maildir/inbox
SHELL=/bin/sh
LOGFILE=$HOME/.procmail.log
DEBUG=yes
VERBOSE=yes

# filter for v-dev
:0:
* ^(From|Cc|To).*v-dev
v-dev/


# all other mails will be moved to the inbox
:0
inbox/
    """


class NotmuchRC(DotFile):
    def __init__(self):
        fs.mkdir_p("%s/.maildir/.notmuch" % HOME)

    def directory(self):
        return HOME

    def file_name(self):
        return ".notmuch-config"

    def post_process_hook(self):
        shell.run_subprocess("notmuch new")

    def template(self):
        tpl = Template("""
[database]
path=$home/.maildir

[user]
name=$user
primary_email=$email

[new]
tags=unread;inbox;
ignore=

[search]
exclude_tags=deleted;spam;

[maildir]
synchronize_flags=true

[crypto]
gpg_path=gpg
    """)
        return tpl.substitute(email=git.get_git_email(),
                              user=git.get_git_user(),
                              home=HOME)


class Mailcap(DotFile):
    def directory(self):
        return HOME

    def file_name(self):
        return ".mailcap"

    def template(self):
        return """
text/html; w3m -I %{charset} -T text/html -o display_link_number=1; copiousoutput;
image/*; feh %s
        """


class MuttSavePatch(DotFile):
    def directory(self):
        return HOME

    def file_name(self):
        return ".mutt_save_email.bash"

    def permissions(self):
        return DEFAULT_PERMS | stat.S_IXUSR

    def template(self):
        return """
#!/bin/bash

# Save piped email to "$1/YYYY-MM-DD SUBJECT.eml"

set -o errexit
set -o nounset
set -o pipefail
set -o noclobber

message=$(cat)

mail_date=$(<<<"$message" grep -oPm 1 '^Date: ?\K.*')
formatted_date=$(date -d"$mail_date" +%Y-%m-%d)
# Get the first line of the subject, and change / to ∕ so it's not a subdirectory
subject=$(<<<"$message" grep -oPm 1 '^Subject: ?\K.*' | sed 's,/,∕,g')

if [[ $formatted_date == '' ]]; then
  echo Error: no date parsed
  exit 1
elif [[ $subject == '' ]]; then
  echo Warning: no subject found
fi

echo "${message}" > "$1/$formatted_date $subject.eml"
echo Email saved to "$1/$formatted_date $subject.eml"
        """


class MuttRC(DotFile):
    def __init__(self, password):
        mboxes = [".", "inbox", "v-dev", "sent", "notmuch"]
        maildir = ["cur", "new", "tmp"]
        for box in mboxes:
            for m in maildir:
                fs.mkdir_p("%s/.maildir/%s/%s" % (HOME, box, m))
        self.password = password

    def directory(self):
        return HOME

    def file_name(self):
        return ".muttrc"

    def template(self):
        tpl = Template("""
set ssl_starttls=yes
set ssl_force_tls=yes
set realname='$user'

set mbox_type=Maildir
set folder = '~/.maildir'
set spoolfile=+inbox
set record=+sent

set header_cache = "~/.maildir/cache/headers"
set message_cachedir = "~/.maildir/cache/bodies"
set certificate_file = "~/.maildir/certificates"
set mailcap_path     = ~/.mailcap
set move = no
set copy = yes
set tmpdir='~/.tmp'


# better ergonomics

set sort             = 'threads'
set sort_aux         = 'reverse-last-date-received'
set auto_tag         = yes

set fast_reply=yes  # don't prompt
set include=yes     # include sender
set pager_stop=yes  # don't go to next email
set pager_context=3 # 3 lines
set sleep_time = 0
set smart_wrap

auto_view text
alternative_order text/plain text/enriched text/html     # save html for last
macro index G "!getmail\\n"
macro pager G "!fetchmail\\n"
#scroll inside the message rather than the index
bind pager <up> previous-line
bind pager <down> next-line
macro index,pager S "| $home/.mutt_save_email.bash $home/Downloads<enter>"

# vectorized
set imap_user = '$email'
set imap_pass = '$password'
set from      = '$email'
set postponed = "imaps://imap.gmail.com/[Gmail]/Drafts"
set smtp_url  = 'smtps://$email@smtp.gmail.com'
set smtp_pass = '$password'
""")
        return tpl.substitute(email=git.get_git_email(),
                              user=git.get_git_user(),
                              home=HOME,
                              password=self.password)


def _render_templates(password):
    dotfiles = [
        SystemdGetmailTimer(),  # background cron
        SystemdGetmailService(),  # background cron
        GetmailRC(password),  # fetcher
        ProcmailRC(),  # post process/sort
        NotmuchRC(),  # indexer
        MuttRC(password),  # viewer
        Mailcap(),
        MuttSavePatch(),
    ]
    for srvc in dotfiles:
        logger.info(f"creating directory {srvc.directory()}")
        fs.mkdir_p(srvc.directory())
        path = "%s/%s" % (srvc.directory(), srvc.file_name())
        if os.path.exists(path):
            logger.info(f"{path} already exists. removing...")
            os.remove(path)
        logger.info(f"creating {path}")
        with open(path, 'w+') as f:
            f.write(srvc.template())
        # ensure correct perms
        os.chmod(path, srvc.permissions())

    for srvc in dotfiles:
        srvc.post_process_hook()


def _build_parser():
    parser = argparse.ArgumentParser(description='Git helper')
    parser.add_argument('--log',
                        type=str,
                        default='INFO',
                        help='info, debug, type log levels. i.e: --log=debug')
    return parser


def main():
    parser = _build_parser()
    options = parser.parse_args()
    log.set_logger_for_main(getattr(logging, options.log.upper()))
    logger.info("%s" % options)

    password = getpass.getpass(prompt='vectorized email app-password: ')
    _render_templates(password)


if __name__ == '__main__':
    main()

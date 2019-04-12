# Sending Patches

Send your changes as patches to the [mailing list](https://groups.google.com/a/vectorized.io/forum/#!forum/v-dev). 
We don't accept pull requests on GitHub.

You commit to your git tree and then prepare patches that you
send to the mailing list.

A single patch should do one, preferably small thing. This helps
reviewers keep only the needed context in their heads, and helps to find bugs
when bisecting. If there is a set of related things that must done, a patch
series should be crafted, where each patch obeys the
do-one-thing criteria.

Each logical change should go into a separate patch, regardless of how many
files it touches. Things like code movement, refactoring or cleanup should
always appear in their own separate patches, so the reviewer knows to do a more
perfunctory review. Each patch should stand on its own, but it can naturally be
built on the preceding ones of the series. Special care should be taken to
ensure no patch introduces a regression.


This is very helpful in learning to create good patches:
https://www.kernel.org/doc/html/v4.19/process/submitting-patches.html

## Configuring git

Configure your name and email address:

```
$ git config user.name "Your Name"
$ git config user.email "your@vectorized.io"
```

Also configure git to detect renames and copies to make ``git format-patch`` output easier to review:

```
git config --global diff.renames copies
```

Run `tools/verify_git_cfg.sh` to setup the .gitorderfile and
configure commit messages to always include the "Signed-off-by" tag.

## Commiting your changes

Your modifications are made in your own private branch:

```
$ git checkout -b features/foo # branches from master
```

To commit changes, do:

```
$ git commit -a # commit everything
```

Please prepare a commit message for every commit:

http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html

## Preparing the patches

Once you have commits you want to send out, use ``git format-patch`` to
generate them.

For multiple patches, use:

```
$ git format-patch -v1 -<N> --cover-letter -o patches
```

where N is the number of commits to include. In the cover letter, include an
overview about the series, and a URL pointing to a git tree that can be pulled 
from and merged into the upstream repo.

For a single patch, do:

```
$ git format-patch -v1 -1
```

## Sending the patches

Verify the generated patch files and then use git send-email to send them out:

```
$ git send-email --suppress-cc=self --to v-dev@vectorized.io 00*.patch
```

Alternatively, to send patches in a directory:

```
$ git send-email --suppress-cc=self --to v-dev@vectorized.io patches/
```

You can find information on how to configure git to use gmail here: [Configuring
git send-email to use Gmail SMTP](http://morefedora.blogspot.com/2009/02/configuring-git-send-email-to-use-gmail.html).

You can copy paste the following to ``.git/config``:

```
[sendemail]
    from = <your name>
    smtpserver = smtp.gmail.com
    smtpuser = <you>@vectorized.io
    smtpencryption = tls
    chainreplyto = false
    smtpserverport = 587
```

You can configure ad-hoc app passwords [here](https://myaccount.google.com/apppasswords).

Copy the ad-hoc password and modify git configuration as follows:

```
$ git config sendemail.smtppass "<password>"
```

## Revisions

If you need to send more than one revision of the patch, please remember to bump
up the version number in the ``-v`` command line option:

```
$ git format-patch -v2 ...
```

Add a brief summary of changes in the new version, for example:

```
In v3:
    - declared move constructor and move assignment operator as noexcept
    - used std::variant instead of a union
    ...
```

The summary should appear in the cover letter, below the overview, or, in case
of a single patch, below the `---` separator, which denotes the content that is
ignored by git when merging.

That patch files need to be edited on occasion is a reason to have individual format
and email sending steps.

## RFCs

In case you want to obtain early feedback on a patch (e.g., if you are unsure
about the design), you can send patches as an RFC by adding the "--rfc" switch
to ``git format-patch``.

## Referencing issues

If the patch or patch series address an issue, include a tag in the commit
message specifying which issue is fixed:

```
Fixes #124
```

Aside from the commit message of the individual patch that fixes the issue, also
add the tag to the cover letter (if applicable).

## Tools

This workflow works best when using Thunderbird or Mutt. 

# Reporting an issue

Please use the [Issue Tracker](https://github.com/vectorizedio/v/issues/) to
report issues.  Fill in as much information as you can, especially for performance problems.

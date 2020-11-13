# Sending Patches

# Overview

Welcome! thanks for looking at our contributing guide. Please be sure
to checkout our [code of conduct](CODE_OF_CONDUCT.md). We work hard to make this
a welcoming community for all.

We currently use the fork+Pull Request model. Please be sure to raise an issue
and brainstorm larger patch series. We care a great deal about the hygene of the
patches that get merged into `/dev` and eventually the `/release` branches. 
We ask that you format your subject lines to 79chars max and that body of your 
patches do not exceed 80 chars.

The c++ code should be formatted with clang-format-11. 
NodeJS (typescript, javascript, etc) with prettier.
Python with yapf.

## RFCs

In case you want to obtain early feedback on a patch (e.g., if you are unsure
about the design), you can send patches as an RFC by adding the "--rfc" switch
to ``git format-patch`` or to solicit discussion to our [mailing list](https://groups.google.com/g/redpanda-users)

## Referencing issues

If the patch or patch series address an issue, include a tag in the commit
message specifying which issue is fixed:

```
Fixes #124
```

Aside from the commit message of the individual patch that fixes the issue, also
add the tag to the cover letter (if applicable). See the list of available keywords
to reference an issue
[here](https://help.github.com/en/articles/closing-issues-using-keywords).


# Reporting an issue

Please use the [Issue Tracker](https://github.com/vectorizedio/redpanda/issues/) to
report issues.  Fill in as much information as you can, especially for performance problems.

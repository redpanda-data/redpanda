
# Migrators

## What is this?

A migrator class listens for the status of a particular feature
in the feature table, and responds to the `preparing` state by
taking some necessary data migration action.  This might mean
rewriting an old data format to a new format, or tweaking a 
configuration from a legacy system to fit a new configuration
scheme.

The name comes from the analogy with database migrations.

## Why are they here?

Migrators are segregated in their own directory because they
are intrinsically temporary code: after a safe number of releases
have passed, we may retire them.

This enables adding & removing migrators without disrupting the
areas of code they migrate data on behalf of.


# Transform Verifier

A small golang program that verifies that data transforms work as expected. 

Most tests for Redpanda today don't inspect the payload, nor do they work on reading a set of messages from a different topic/partition than they were originally produced on.

See the help text for more information on how to use the commands.

### Development

A few notes as you read the code:

1. Any global state is attached to the context. This allows easy access to sharing state in a structured way that would allow for testing and prevents a proliferation of shared global variables between packages.
2. Everything is structured for graceful shutdown. Again, code should check the context to see if things are done when working in a loop or waiting. This allows for graceful shutdown. As we want ducktape to be in control of when the process exits so it can capture the latest state before shutting down. So there is an endpoint to shutdown the process. All this is wired through the context.
3. At the time of writing, this only supports verifying "mirror" or "noop" transforms, but over time we can perform more validation for other interesting kinds of transforms. When adding new validation, keep in mind that transforms ensure at-least-once delivery, so care should be taken to allow for these edge cases when duplicates are emitted, as they are valid.


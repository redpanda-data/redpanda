# Schema Library

This library contains functionality to interact with the schemas in the Schema Registry, hiding a bulk of the internal details found within `pandaproxy::schema_registry`. Developers should consider using this instead of depending directly on `pandaproxy` if their code only needs application-facing primitives like schemas, schema IDs, subjects, etc., and not e.g. specific HTTP endpoints or the `_schemas` topic.

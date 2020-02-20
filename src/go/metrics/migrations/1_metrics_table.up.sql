CREATE TABLE IF NOT EXISTS metrics (
  sent_at        timestamp,
  received_at    timestamp,
  organization   varchar,
  cluster_id     varchar,
  node_id        integer,
  node_uuid      varchar,
  free_memory_mb numeric,
  free_space_mb  numeric,
  cpu_percentage numeric
);
CREATE INDEX metrics_received_at_idx ON metrics (received_at);

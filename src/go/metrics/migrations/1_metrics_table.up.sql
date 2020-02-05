CREATE TABLE IF NOT EXISTS metrics (
  sent_at        timestamp,
  received_at    timestamp,
  organization   varchar,
  cluster_id     varchar,
  node_id        integer,
  node_uuid      varchar,
  free_memory    integer,
  free_space     integer,
  cpu_percentage integer
);
CREATE INDEX metrics_received_at_idx ON metrics (received_at);

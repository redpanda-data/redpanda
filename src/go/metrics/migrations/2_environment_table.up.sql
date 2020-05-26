CREATE TABLE IF NOT EXISTS environment (
  sent_at        timestamp not null,
  received_at    timestamp not null,
  organization   varchar not null,
  cluster_id     varchar not null,
  node_id        integer not null,
  node_uuid      varchar not null,
  payload        jsonb,
  config         jsonb
);

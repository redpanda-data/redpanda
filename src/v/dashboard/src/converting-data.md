# FROM (prometheus export)

```json
{
  name: "vectorized_storage_log_read_bytes",
  help: "Total number of bytes read",
  timestamp: 1604615050592,
  type: "COUNTER",
  metrics: [
    {
      labels: { namespace: "redpanda", partition: "0", shard: "0", topic: "controller" }
      value: "3159"
    }
  ]
}
```

# TO (react-charts.js format)

```json
{
  "label": "Series 2",
  "datums": [
    {
      "x": new Date("2020-03-18T11:00:00.000Z"),
      "y": 41
    },
    {
      "x": new Date("2020-03-18T11:30:00.000Z"),
      "y": 15
    }
  ]
}
```

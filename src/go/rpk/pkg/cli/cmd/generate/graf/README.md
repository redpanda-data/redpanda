# graf

`graf` is a Go representation of the
[Grafana Dashboard JSON model][grafana-json-model].

Its main reason is to allow building and generating custom dashboards in Go and
exporting them to JSON so that they can be imported in Grafana.

Currently supported panels: row, graph, text, singlestat.

There are constructors for all the panels, which provide sensible default values
for each type & sequential ID assignment.

## Usage

```go
datasource := "prometheus"
graph := graf.NewGraphPanel("graph panel 1", "ops")
graph.ID = 1
graph.Datasource = datasource
graph.Targets = []graf.Target{graf.Target{
   	Expr:           `sum(metric{instance=~"[[ip]]") by ([[aggr_criteria]])`,
   	LegendFormat:   `instance: {{instance}}`,
   	Format:         "time_series",
   	Step:           10,
   	IntervalFactor: 2,
}}
graph.GetGridPos().X = 0
graph.GetGridPos().Y = 0

graph.Tooltip = graf.Tooltip{
   	MsResolution: true,
	Shared:       true,
	ValueType:    "cumulative",
}
singlestat := graf.NewGraphPanel("single stat 1", "bytes")
singlestat.ID = 2
singlestat.Datasource = datasource
singlestat.Interval = "2m"
singlestat.NullPointMode = "connected"
singlestat.Targets = []graf.Target{graf.Target{
   	Expr: `count(up{job=~"node"})`,
}}
singlestat.GetGridPos().X = 8
singlestat.GetGridPos().Y = 6

panels := []graf.Panel{graf.NewRowPanel("row 1", graph, singlestat)}
timePicker := graf.TimePicker{
   	RefreshIntervals: []string{"10s", "1m", "5m", "15m", "1h"},
   	TimeOptions:      []string{"5m", "15m", "1h", "1d", "7d"},
}
dashboard := graf.Dashboard{
   	Title: "Redpanda",
	Templating: graf.Templating{
	   	List: []graf.TemplateVar{graf.TemplateVar{
		   	Name:       "var 1",
		   	Datasource: datasource,
		   	Label:      "label 1",
		   	Multi:      true,
		   	Refresh:    1,
		   	Sort:       1,
		   	Options: []graf.Option{
		   		graf.Option{
		   			Text:  "Cluster",
		   			Value: "",
		   		},
		   		graf.Option{
		   			Text:  "Instance",
		   			Value: "instance,",
		   		},
		   	},
	   	}},
   	},
	Panels:        panels,
	Editable:      true,
	Refresh:       "10s",
	Time:          graf.Time{From: "now-1h", To: "now"},
	TimePicker:    timePicker,
	Timezone:      "utc",
	SchemaVersion: 12,
}

jsonModel, _ := json.Marshal(dashboard)
```

## Things to keep in mind

### Grid system

As seen in the example above, it's up to you to set each panel's
[GridPos][grafana-grid] `X` & `Y` values. If you don't, Grafana will put all the
panels in the same coordinates ((0, 0) by default) and the dashboard won't work.
You can read more about Grafana's grid system in the
[Grafana docs][grafana-grid].

### Panel IDs

`graf` assigns an auto-incremented ID to each panel if it's created through a
constructor (e.g. `NewGraphPanel`). However, you're free to use your own IDs,
or to create the panels directly. In that case, make sure you assign a unique ID
to each; Grafana doesn't auto-assign panel IDs when a dashboard is imported, and
it doesn't support 2 panels having the same ID, which causes the whole dashboard
to break.

It's also worth noting that IDs with very high values may break the dashboard
when you import it, so limit their range if you're assigning random IDs.


## Similar libs
- [https://github.com/grafana-tools/sdk](grafana-tools/sdk)


[grafana-grid](https://grafana.com/docs/grafana/latest/reference/dashboard/#panel-size-and-position)

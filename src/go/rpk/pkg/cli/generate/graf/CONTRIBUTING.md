# Hacking

The idea is to keep this lib as light as possible, only including what's needed
to be able to generate working dashboards.

## Adding new panels/ dashboard elements

There's very little documentation about the dashboard structure beyond the basic
[JSON model][grafana-json-model] docs. There's no documentation for specific
panels, such as graph, text, etc., so it's difficult to know which fields are
required or optional, what their schema is, the allowed values for certain
string fields. Because of that, the suggested way to go about extending `graf`
is to first create an instance of the thing you wanna add manually in an
existing dashboard, then exporting the dashboard as JSON, and adding the
relevant fields, structs and relationships in the code.

For example, let's say you want to add support for a new type of panel, called
3D. You would then start a new Grafana instance or go to an existing one, create
a new dashboard, and manually add a 3D panel. Then, you would save and export
the dashboard as JSON, and isolate the relevant object. For example, let's say
this is the panel JSON object, extracted from the exported dashboard:

```json
{
  "type": "3d",
  "id": 1,
  "title": "New Panel",
  "datasource": "prometheus",
  "editable": true,
  "gridPos": {"h": 8,"w": 8,"x": 0,"y": 0},
  "links": null,
  "targets": [
    {
      "refId": "A",
      "expr": "some_expr{filter1=\"value\"}",
      "legendFormat": "legend",
      "format": "newformat"
    }
  ],
  "xaxis": {
    "logBase": 0,
    "show": true,
    "mode": "time"
  },
  "yaxes": [
    {
      "show": true,
      "logBase": 1,
      "min": 0,
      "format": "µs"
    }
  ],
  "zaxis": {
    "show": true,
    "logBase": 1,
    "min": 0,
    "format": "short"
  },
  extraField: {},
  "legend": {"show": true},
  "fill": 1,
  "linewidth": 2,
  "nullPointMode": "null",
  "lines": true,
  "tooltip": {
    "shared": true,
    "value_type": "individual",
    "msResolution": true
  }
}
```

The first thing you can do is identifying the changes in the overall structure.
Fields such as `id` or `editable` aren't new and are already present in the
[`BasePanel`](dashboard.go) struct. There are 3 "new" things: the `type` of the
panel, a `zaxis` object and an `extraField` empty object field.

Therefore, our new `3DPanel` struct could look like this:

```go
type 3DPanel struct {
    *BasePanel
    ZAxis      ZAxis       `json:"zaxis"`
    ExtraField interface{} `json:"extraField"`
}
```

`BasePanel` is a struct meant to be embedded in other instances of `Panel`. It
comprises all the common fields that all panels have, such as `Editable` and
`GridPos`. It should only be changed when there's a new field that's shared
among all panel types.

The next step would be to implement the [`Panel`](dashboard.go) interface
which requires that the panel have a `Type` func and a `GetGridPos` func.

`Type` is a way to provide a "constant" type for each `Panel` implementation,
since Go doesn't support default values for struct fields. It's not an
unexported field either because it could still be changed by code within the
`graf` package. This would be `3DPanel`'s `Type`:

```go
func (*3DPanel) Type() string {
    return "3d" 
}
```

`GetGridPos` is needed in case the library user wants to set a panel's gridpos
after creating it. It's implementation is very straightforward:

```go
func (p *3DPanel) GetGridPos() *GridPos {
    return &p.GridPos
}
```

We also need to create our own custom `MarshalJSON`:

```go
func (p *3DPanel) MarshalJSON() ([]byte, error) {
	type PanelAlias 3DPanel
	typedPanel := struct {
		Type string `json:"type"`
		PanelAlias
	}{
		p.Type(),
		(PanelAlias)(*p),
	}
	return json.Marshal(typedPanel)
}
```

There's a lot happening here, so let's unpack it.

First of all, we're creating an anonymous struct and adding a `Type` field.
We do this to add any fields that should go in the resulting JSON, but which
aren't present in the struct, like `type`.

Additionally, we're declaring an alias for our 3DPanel type. If we didn't, Go
would call `MarshalJSON` recursively. `MarshalJSON` is declared for `3DPanel`,
not for `PanelAlias`, so when marshaling the anonymous struct it doesn't call
the same function, but treats it like a regular value that doesn't have a custom
`MarshalJSON` override.

Then try importing a dashboard containing a `3d` panel. Hopefully it will work!
If it doesn't, repeat the process and try to identify any fields you might have
missed, or default values that should change.

If it does work, try to clean up the struct, removing any seemingly useless
fields. In our example that would be `extraField`. Test again after removing it,
and so on. In the end, only the minimum required should remain.

This is needed because Grafana's JSON schema aims to be backwards compatible,
and seems to have lots of fields that are remnants of previous versions. They
need to be weeded out because when the dashboard is imported in Grafana, it's
passed through the
["migrator"](https://github.com/grafana/grafana/blob/master/public/app/features/dashboard/state/DashboardMigrator.ts),
which is a script that converts dashboards using old JSON schemas into ones
compatible with the new one. It's full of heuristics and conditions that are
sometimes triggered by the presence of certain fields. So if unneeded fields are
left, the script might deduce the wrong schema version and break other things.

[grafana-json-model](https://grafana.com/docs/grafana/latest/reference/dashboard/)

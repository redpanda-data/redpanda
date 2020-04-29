package generate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"

	"github.com/grafana-tools/sdk"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type NoopFormatter struct{}

func (*NoopFormatter) Format(e *log.Entry) ([]byte, error) {
	return []byte(e.Message), nil
}

func datasource() *string {
	ds := "prometheus"
	return &ds
}

var panelHeight = "250px"

var metricGroups = []string{
	"storage",
	"reactor",
	"scheduler",
	"io_queue",
	"vectorized_internal_rpc_protocol",
	"kafka_rpc_protocol",
	"rpc_client",
	"memory",
}

func NewGrafanaDashboardCmd() *cobra.Command {
	var prometheusURL string
	command := &cobra.Command{
		Use:   "grafana-dashboard [--src-url]",
		Short: "Generate a Grafana dashboard for redpanda metrics.",
		RunE: func(ccmd *cobra.Command, args []string) error {
			if !(strings.HasPrefix(prometheusURL, "http://") ||
				strings.HasPrefix(prometheusURL, "https://")) {
				prometheusURL = fmt.Sprintf("http://%s", prometheusURL)
			}
			return executeGrafanaDashboard(prometheusURL)
		},
	}
	prometheusURLFlag := "prometheus-url"
	command.Flags().StringVar(
		&prometheusURL,
		prometheusURLFlag,
		"http://localhost:9644/metrics",
		"The redpanda Prometheus URL from where to get the metrics metadata")
	return command
}

func metricGroup(metric string) string {
	for _, group := range metricGroups {
		if strings.Contains(metric, group) {
			return group
		}
	}
	return "others"
}

func executeGrafanaDashboard(prometheusURL string) error {
	metricFamilies, err := fetchMetrics(prometheusURL)
	if err != nil {
		return err
	}
	dashboard := buildGrafanaDashboard(metricFamilies)
	jsonSpec, err := json.MarshalIndent(dashboard, "", " ")
	if err != nil {
		return err
	}
	log.SetFormatter(&NoopFormatter{})
	log.Info(string(jsonSpec))
	return nil
}

func buildGrafanaDashboard(
	metricFamilies map[string]*dto.MetricFamily,
) *sdk.Board {
	groupPanels := map[string][]sdk.Panel{}
	panelID := uint(0)

	names := []string{}
	for k, _ := range metricFamilies {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		family := metricFamilies[name]
		var panel *sdk.Panel
		if family.GetType() == dto.MetricType_COUNTER {
			panel = newCounterPanel(family)
		} else if subtype(family) != "histogram" {
			panel = newGaugePanel(family)
		}
		if panel != nil {
			panel.ID = panelID
			panelID++
			group := metricGroup(name)
			panels, ok := groupPanels[group]
			if ok {
				groupPanels[group] = append(panels, *panel)
			} else {
				groupPanels[group] = []sdk.Panel{*panel}
			}
		}
	}

	rows := []*sdk.Row{}
	for group, panels := range groupPanels {
		row := &sdk.Row{
			Title:     group,
			ShowTitle: true,
			Panels:    panels,
			Editable:  true,
			Height:    sdk.Height(panelHeight),
		}
		rows = append(rows, row)
	}

	node := newTemplateVar("node", "Node", "label_values(instance)")
	shard := newTemplateVar("node_shard", "shard", "label_values(shard)")
	templating := sdk.Templating{List: []sdk.TemplateVar{node, shard}}

	dashboard := sdk.NewBoard("Redpanda")
	dashboard.Templating = templating
	dashboard.Rows = rows
	dashboard.Refresh = &sdk.BoolString{Flag: true, Value: "10s"}
	dashboard.Time = sdk.Time{From: "now-1h", To: "now"}
	dashboard.Timepicker = sdk.Timepicker{
		RefreshIntervals: []string{
			"5s",
			"10s",
			"30s",
			"1m",
			"5m",
			"15m",
			"30m",
			"1h",
			"2h",
			"1d",
		},
		TimeOptions: []string{
			"5m",
			"15m",
			"1h",
			"6h",
			"12h",
			"24h",
			"2d",
			"7d",
			"30d",
		},
	}
	dashboard.SchemaVersion = 12
	dashboard.Timezone = "utc"
	return dashboard
}

func fetchMetrics(prometheusURL string) (map[string]*dto.MetricFamily, error) {
	res, err := http.Get(prometheusURL)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, fmt.Errorf(
			"the request to %s failed. Status: %d",
			prometheusURL,
			res.StatusCode,
		)
	}
	bs, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	parser := &expfmt.TextParser{}
	return parser.TextToMetricFamilies(bytes.NewBuffer(bs))
}

func newCounterPanel(m *dto.MetricFamily) *sdk.Panel {
	expr := fmt.Sprintf(
		`irate(%s{instance=~"[[node]]",shard=~"[[node_shard]]"}[1m])`,
		m.GetName(),
	)
	format := "ops"
	if strings.Contains(subtype(m), "bytes") {
		format = "bytes"
	}
	panel := newGraphPanel(m, "Rate - "+m.GetHelp(), expr, format)
	panel.Lines = true
	return panel
}

func newGaugePanel(m *dto.MetricFamily) *sdk.Panel {
	expr := fmt.Sprintf(
		`%s{instance=~"[[node]]",shard=~"[[node_shard]]"}`,
		m.GetName(),
	)
	format := "short"
	if strings.Contains(subtype(m), "bytes") {
		format = "bytes"
	}
	panel := newGraphPanel(m, m.GetHelp(), expr, format)
	panel.Bars = true
	panel.Lines = false
	return panel
}

func defaultTarget(family *dto.MetricFamily, expr string) sdk.Target {
	return sdk.Target{
		Expr:           expr,
		LegendFormat:   legendFormat(family),
		Format:         "time_series",
		Step:           10,
		IntervalFactor: 2,
	}
}

func newGraphPanel(
	family *dto.MetricFamily, title,
	targetExpr,
	yAxisFormat string,
) *sdk.Panel {
	panel := sdk.NewGraph(title)
	panel.GraphPanel.Targets = []sdk.Target{
		defaultTarget(family, targetExpr),
	}
	panel.Legend = sdk.Legend{Show: true}
	panel.Datasource = datasource()
	panel.Fill = 1
	panel.Linewidth = 2
	panel.Editable = true
	panel.Span = 4
	panel.AliasColors = struct{}{}
	panel.Tooltip = sdk.Tooltip{
		MsResolution: true,
		Shared:       true,
		Sort:         0,
		ValueType:    "cumulative",
	}
	panel.Xaxis = sdk.Axis{Show: true}
	yAxis := sdk.Axis{
		Format:  yAxisFormat,
		LogBase: 1,
		Show:    true,
		Min:     &sdk.FloatString{Value: 0.0, Valid: true},
	}
	yAxis1 := sdk.Axis{
		Format:  "short",
		LogBase: yAxis.LogBase,
		Show:    yAxis.Show,
		Min:     yAxis.Min,
	}
	panel.Yaxes = []sdk.Axis{yAxis, yAxis1}
	return panel
}

func newTemplateVar(name, label, query string) sdk.TemplateVar {
	var refresh int64 = 1
	return sdk.TemplateVar{
		Name:       name,
		Datasource: datasource(),
		Label:      label,
		Query:      query,
		Type:       "query",
		Current:    sdk.Current{Tags: []*string{}},
		Multi:      true,
		Refresh:    sdk.BoolInt{Flag: true, Value: &refresh},
		Sort:       1,
		Options:    []sdk.Option{},
	}
}

func legendFormat(m *dto.MetricFamily) string {
	duplicate := func(s string, ls []string) bool {
		for _, l := range ls {
			if s == l {
				return true
			}
		}
		return false
	}
	labels := []string{}
	legend := "node: {{instance}}"
	for _, metric := range m.GetMetric() {
		for _, label := range metric.GetLabel() {
			name := label.GetName()
			if name != "type" && !duplicate(name, labels) {
				legend += fmt.Sprintf(
					", %s: {{%s}}",
					name,
					name,
				)
				labels = append(labels, name)
			}
		}
	}
	return legend
}

func subtype(m *dto.MetricFamily) string {
	for _, metric := range m.GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "type" {
				return label.GetValue()
			}
		}
	}
	return "none"
}

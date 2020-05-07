package generate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"vectorized/pkg/cli/cmd/generate/graf"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

type NoopFormatter struct{}

func (*NoopFormatter) Format(e *log.Entry) ([]byte, error) {
	return []byte(e.Message), nil
}

const datasource = "prometheus"

const panelHeight = "250px"

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
	// The logger's default stream is stderr, which prevents piping to files
	// from working without redirecting them with '2>&1'.
	if log.StandardLogger().Out == os.Stderr {
		log.SetOutput(os.Stdout)
	}
	log.Info(string(jsonSpec))
	return nil
}

func buildGrafanaDashboard(
	metricFamilies map[string]*dto.MetricFamily,
) graf.Dashboard {
	groupPanels := map[string][]graf.Panel{}

	names := []string{}
	for k, _ := range metricFamilies {
		names = append(names, k)
	}
	sort.Strings(names)
	id := uint(0)
	for _, name := range names {
		family := metricFamilies[name]
		var panel graf.Panel
		if family.GetType() == dto.MetricType_COUNTER {
			panel = newCounterPanel(family, id)
		} else if subtype(family) != "histogram" {
			panel = newGaugePanel(family, id)
		}
		if panel != nil {
			id++
			group := metricGroup(name)
			panels, ok := groupPanels[group]
			if ok {
				groupPanels[group] = append(panels, panel)
			} else {
				groupPanels[group] = []graf.Panel{panel}
			}
		}
	}

	rowTitles := []string{}
	rowsByTitle := map[string]graf.Row{}
	for group, panels := range groupPanels {
		rowsByTitle[group] = graf.NewRow(group, panels, true)
		rowTitles = append(rowTitles, group)
	}

	sort.Strings(rowTitles)

	rows := []graf.Row{}
	for _, title := range rowTitles {
		rows = append(rows, rowsByTitle[title])
	}

	node := newDefaultTemplateVar("node", "Node", true)
	node.IncludeAll = true
	node.AllValue = ".*"
	node.Type = "query"
	node.Query = "label_values(instance)"
	shard := newDefaultTemplateVar("node_shard", "shard", true)
	shard.IncludeAll = true
	shard.AllValue = ".*"
	shard.Type = "query"
	shard.Query = "label_values(shard)"
	clusterOpt := graf.Option{
		Text:     "Cluster",
		Value:    "sum",
		Selected: false,
	}
	aggregateOpts := []graf.Option{
		graf.Option{
			Text:     "None",
			Value:    "",
			Selected: true,
		},
		graf.Option{
			Text:     "Instance",
			Value:    "sum by(instance)",
			Selected: false,
		},
		clusterOpt,
	}
	aggregate := newDefaultTemplateVar("aggregate", "Aggregate by", false, aggregateOpts...)
	aggregate.Type = "custom"
	aggregate.Current = graf.Current{
		Text:  clusterOpt.Text,
		Value: clusterOpt.Value,
	}

	return graf.Dashboard{
		Title: "Redpanda",
		Templating: graf.Templating{
			List: []graf.TemplateVar{node, shard, aggregate},
		},
		Rows:     rows,
		Editable: true,
		Refresh:  "10s",
		Time:     graf.Time{From: "now-1h", To: "now"},
		TimePicker: graf.TimePicker{
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
		},
		Timezone:      "utc",
		SchemaVersion: 12,
	}
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

func newCounterPanel(m *dto.MetricFamily, id uint) graf.GraphPanel {
	expr := fmt.Sprintf(
		`[[aggregate]] (irate(%s{instance=~"[[node]]",shard=~"[[node_shard]]"}[1m]))`,
		m.GetName(),
	)
	target := graf.Target{
		Expr:           expr,
		LegendFormat:   legendFormat(m),
		Format:         "time_series",
		Step:           10,
		IntervalFactor: 2,
	}
	format := "ops"
	if strings.Contains(subtype(m), "bytes") {
		format = "Bps"
	}
	panel := newGraphPanel(id, "Rate - "+m.GetHelp(), target, format)
	panel.Lines = true
	return panel
}

func newGaugePanel(m *dto.MetricFamily, id uint) graf.GraphPanel {
	expr := fmt.Sprintf(
		`[[aggregate]] (%s{instance=~"[[node]]",shard=~"[[node_shard]]"})`,
		m.GetName(),
	)
	target := graf.Target{
		Expr:           expr,
		LegendFormat:   legendFormat(m),
		Format:         "time_series",
		Step:           10,
		IntervalFactor: 2,
	}
	format := "short"
	if strings.Contains(subtype(m), "bytes") {
		format = "bytes"
	}
	panel := newGraphPanel(id, m.GetHelp(), target, format)
	panel.Bars = true
	return panel
}

func newGraphPanel(
	id uint,
	title string,
	target graf.Target,
	yAxisFormat string,
) graf.GraphPanel {
	// yAxisMin := 0.0
	p := graf.NewGraphPanel(title, yAxisFormat)
	p.ID = id
	p.Datasource = datasource
	p.Targets = []graf.Target{target}
	p.Tooltip = graf.Tooltip{
		MsResolution: true,
		Shared:       true,
		ValueType:    "cumulative",
	}
	return p
}

func newDefaultTemplateVar(
	name, label string, multi bool, opts ...graf.Option,
) graf.TemplateVar {
	return graf.TemplateVar{
		Name:       name,
		Datasource: datasource,
		Label:      label,
		Multi:      multi,
		Refresh:    1,
		Sort:       1,
		Options:    opts,
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

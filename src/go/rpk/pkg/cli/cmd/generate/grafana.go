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
	"vectorized/pkg/cli"
	"vectorized/pkg/cli/cmd/generate/graf"
	"vectorized/pkg/utils"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var datasource string

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
	"raft",
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
	datasourceFlag := "datasource"
	command.Flags().StringVar(
		&datasource,
		datasourceFlag,
		"",
		"The name of the Prometheus datasource as configured in your grafana instance.")
	command.MarkFlagRequired(datasourceFlag)
	return command
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
	log.SetFormatter(cli.NewNoopFormatter())
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
	intervals := []string{"5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"}
	timeOptions := []string{"5m", "15m", "1h", "6h", "12h", "24h", "2d", "7d", "30d"}
	return graf.Dashboard{
		Title:      "Redpanda",
		Templating: buildTemplating(),
		Rows:       processRows(metricFamilies),
		Editable:   true,
		Refresh:    "10s",
		Time:       graf.Time{From: "now-1h", To: "now"},
		TimePicker: graf.TimePicker{
			RefreshIntervals: intervals,
			TimeOptions:      timeOptions,
		},
		Timezone:      "utc",
		SchemaVersion: 12,
	}
}

func processRows(metricFamilies map[string]*dto.MetricFamily) []graf.Row {
	groupPanels := map[string][]graf.Panel{}
	throughputMetrics := []string{
		"vectorized_storage_log_read_bytes",
		"vectorized_storage_log_written_bytes",
	}
	latencyPanels := []graf.Panel{}
	throughputPanels := []graf.Panel{}
	summaryRow := graf.NewRow("Summary", []graf.Panel{}, false)
	errorsRow := graf.NewRow("Errors", []graf.Panel{}, true)

	names := []string{}
	for k, _ := range metricFamilies {
		names = append(names, k)
	}
	sort.Strings(names)
	id := uint(0)
	for _, name := range names {
		id++
		family := metricFamilies[name]
		var panel graf.Panel
		if family.GetType() == dto.MetricType_COUNTER {
			panel = newCounterPanel(family, id)
		} else if subtype(family) == "histogram" {
			latencyPanels = append(
				latencyPanels,
				newPercentilePanel(family, id, 0.95),
			)
			continue
		} else {
			panel = newGaugePanel(family, id)
		}

		if panel == nil {
			continue
		}
		if utils.StringInSlice(family.GetName(), throughputMetrics) {
			throughputPanels = append(throughputPanels, panel)
			continue
		}

		if strings.Contains(family.GetName(), "error") {
			errorsRow.Panels = append(
				errorsRow.Panels,
				panel,
			)
			continue
		}
		group := metricGroup(name)
		panels, ok := groupPanels[group]
		if ok {
			groupPanels[group] = append(panels, panel)
		} else {
			groupPanels[group] = []graf.Panel{panel}
		}
	}
	summaryRow.Panels = append(latencyPanels, throughputPanels...)

	rowTitles := []string{}
	rowsByTitle := map[string]graf.Row{}
	for group, panels := range groupPanels {
		rowsByTitle[group] = graf.NewRow(group, panels, true)
		rowTitles = append(rowTitles, group)
	}

	sort.Strings(rowTitles)

	rows := []graf.Row{
		summaryRow,
		errorsRow,
	}
	for _, title := range rowTitles {
		rows = append(rows, rowsByTitle[title])
	}
	return rows
}

func buildTemplating() graf.Templating {
	node := newDefaultTemplateVar("node", "Node", true)
	node.IncludeAll = true
	node.AllValue = ".*"
	node.Type = "query"
	node.Query = "label_values(instance)"
	shard := newDefaultTemplateVar("node_shard", "Shard", true)
	shard.IncludeAll = true
	shard.AllValue = ".*"
	shard.Type = "query"
	shard.Query = "label_values(shard)"
	clusterOpt := graf.Option{
		Text:     "Cluster",
		Value:    "",
		Selected: false,
	}
	aggregateOpts := []graf.Option{
		clusterOpt,
		graf.Option{
			Text:     "Instance",
			Value:    "instance,",
			Selected: false,
		},
		graf.Option{
			Text:     "Instance, Shard",
			Value:    "instance,shard,",
			Selected: false,
		},
	}
	aggregate := newDefaultTemplateVar(
		"aggr_criteria",
		"Aggregate by",
		false,
		aggregateOpts...,
	)
	aggregate.Type = "custom"
	aggregate.Current = graf.Current{
		Text:  clusterOpt.Text,
		Value: clusterOpt.Value,
	}
	return graf.Templating{
		List: []graf.TemplateVar{node, shard, aggregate},
	}
}

func metricGroup(metric string) string {
	for _, group := range metricGroups {
		if strings.Contains(metric, group) {
			return group
		}
	}
	return "others"
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

func newPercentilePanel(
	m *dto.MetricFamily, id uint, percentile float32,
) graf.GraphPanel {
	expr := fmt.Sprintf(
		`histogram_quantile(%.2f, sum(rate(%s_bucket{instance=~"[[node]]",shard=~"[[node_shard]]"}[1m])) by (le, [[aggr_criteria]]))`,
		percentile,
		m.GetName(),
	)
	target := graf.Target{
		Expr:           expr,
		LegendFormat:   legendFormat(m),
		Format:         "time_series",
		Step:           10,
		IntervalFactor: 2,
		RefID:          "A",
	}
	title := fmt.Sprintf("%s (p%.0f)", m.GetHelp(), percentile*100)
	panel := newGraphPanel(id, title, target, "µs")
	panel.Lines = true
	panel.Tooltip.ValueType = "individual"
	panel.Tooltip.Sort = 0
	return panel
}

func newCounterPanel(m *dto.MetricFamily, id uint) graf.GraphPanel {
	expr := fmt.Sprintf(
		`sum(irate(%s{instance=~"[[node]]",shard=~"[[node_shard]]"}[1m])) by ([[aggr_criteria]])`,
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
		`sum(%s{instance=~"[[node]]",shard=~"[[node_shard]]"}) by ([[aggr_criteria]])`,
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
	panel.Lines = true
	panel.SteppedLine = true
	return panel
}

func newGraphPanel(
	id uint, title string, target graf.Target, yAxisFormat string,
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

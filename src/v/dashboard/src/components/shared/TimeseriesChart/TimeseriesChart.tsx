import React, { useState, useMemo, useEffect } from "react";
import "./TimeseriesChart.scss";

// Libs
// @ts-ignore
import Plot from "react-plotly.js";

// Components
import IconButton from "components/shared/IconButton/IconButton";

// Utils
import { Snapshot, Graph } from "types";
import createTitleFromName from "utils/createTitleFromName";

const TimeseriesChart = ({
  graph = { name: "", expanded: false },
  snapshots,
  remove,
  update,
  interval,
  timeWindow = 6,
}: {
  graph: Graph;
  snapshots: Snapshot[];
  remove: (name: string) => void;
  update: (name: string, data: object) => void;
  interval: number;
  timeWindow: number;
}) => {
  // Filter out all metrics !== name
  const filteredSnapshots = filterSnapshotsByName(graph.name, snapshots);
  const info = filteredSnapshots[0]
    ? filteredSnapshots[0].data[0]
    : { help: null }; // Get prometheus data from first in array

  const series = useMemo(
    () => convertSnapshotsToPlotlyData(filteredSnapshots),
    [snapshots]
  );

  const data = [
    {
      x: series.x,
      y: series.y,
      type: "scatter",
      fill: 'tonexty',
      mode: "lines",
    },
  ];

  // Time range for xAxis
  const getTimeRange = () => {
    const time = new Date();
    const olderTime = time.setMilliseconds(time.getMilliseconds() - (timeWindow));
    const futureTime = time.setMilliseconds(time.getMilliseconds() + (timeWindow));
    return [olderTime, futureTime];
  };

  // Chart state
  const toggleExpanded = () =>
    update(graph.name, { expanded: !graph.expanded });
  const [state, setState] = useState({
    data: [
      {
        x: series.x,
        y: series.y,
        type: "scatter",
        mode: "lines",
      },
    ],
    frames: [],
    config: {
      displayModeBar: false,
    },
    layout: {
      autosize: true,
      margin: {
        l: 50,
        r: 20,
        b: 50,
        t: 20,
        pad: 4,
      },
      font: {
        size: 10,
      },
      xaxis: {
        type: "date",
        range: getTimeRange(),
      },
    },
  });

  useEffect(() => {
    setState({
      ...state,
      layout: {
        ...state.layout,
        xaxis: {
          ...state.layout.xaxis,
          range: getTimeRange(),
        },
      },
    });
  }, [snapshots]);

  return (
    <div
      className="TimeseriesChart fade-in-up"
      style={{ gridColumn: graph.expanded ? "1/-1" : "" }}
    >
      <div className="TimeseriesChart-Actions">
        <IconButton
          type={graph.expanded ? "collapse" : "expand"}
          onClick={toggleExpanded}
        />
        <IconButton type="remove" onClick={() => remove(graph.name)} />
      </div>

      <h2
        className="TimeseriesChart-Heading"
        style={{ textTransform: "capitalize" }}
      >
        {createTitleFromName(graph.name)}
      </h2>

      <div style={{ marginTop: 0 }}>
        <AxisLabel item="x">Time</AxisLabel>
        <AxisLabel item="y">{info.help}</AxisLabel>
      </div>

      <div style={{ height: "300px" }}>
        <Plot
          data={data}
          style={{ width: "100%", height: "100%" }}
          useResizeHandler={true}
          frames={state.frames}
          config={state.config}
          layout={state.layout}
        />
      </div>
    </div>
  );
};

const AxisLabel = ({ item, children }: { item: string; children: any }) => (
  <label className="AxisLabel">
    <div className="AxisLabel-Item">{item}</div>
    <div className="AxisLabel-Text">{children}</div>
  </label>
);

const filterSnapshotsByName = (name: string, snapshots: Snapshot[]) => {
  return snapshots.map((snapshot) => ({
    ...snapshot,
    data: snapshot.data.filter((metric) => metric.name === name),
  }));
};

interface PlotlyPoints {
  x: any[];
  y: any[];
}

const convertSnapshotsToPlotlyData = (snapshots: Snapshot[]) => {
  return snapshots.reduce(
    (object: PlotlyPoints, snapshot) => {
      const value = parseFloat(snapshot.data[0].metrics[0].value) || 0;
      object.x.push(new Date(snapshot.timestamp));
      object.y.push(value);
      return object;
    },
    { x: [], y: [] }
  );
};

export default TimeseriesChart;

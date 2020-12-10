import React, { useState, useEffect } from "react";

// README
// When ready to use live data find NOTE 0, NOTE 1, NOTE 2 below.

// Test data
import testSnapshot from "../prometheus-test-snapshot";

// Libs
// @ts-ignore
import parsePrometheusTextFormat from "parse-prometheus-text-format";

// Utils
import { Metric, Snapshot } from "../types";
import fetchPrometheusSnapshot from "../utils/fetchPrometheusSnapshot";
import getRandomInt from "utils/getRandomInt";

const useSnapshots = (
  count: number,
  setCount: (count: number) => void,
  useTestData: boolean = true // NOTE 0 - change this to false
) => {
  // Limit the amount of snapshots that the browser retains in memory
  const limit = 500;

  // Snapshot data
  const [snapshots, setSnapshots] = useState<any[]>([]);

  const resetSnapshots = () => {
    setSnapshots([]);
    setCount(0);
  };

  const generateTestData = (data: Metric[]) => {
    const fakeMetric = (metric: Metric) => ({
      ...metric,
      metrics: [{ ...metric.metrics[0], value: getRandomInt(1000, 3000) }],
    });
    return data.map((metric) => fakeMetric(metric));
  };

  const addTimestamp = (data: Metric[]) => ({
    timestamp: Date.now(),
    data: useTestData ? generateTestData(data) : data,
  });

  const updateSnapshots = () => {
    // NOTE 1 - Enable this
    // let parsedSnapshot = undefined
    // fetchSnapshotTest()
    //   .then(response => response.text())
    //   .then(snapshots => { parsedSnapshot = snapshots })
    //   .catch(error => console.log(error))

    // NOTE 2 - Disable this
    const parsedSnapshot = parsePrometheusTextFormat(testSnapshot);

    // Append new snapshot + add timestamp
    const existingSnapshots =
      snapshots.length < limit
        ? snapshots
        : snapshots.filter((item, i) => i !== 0);
    setSnapshots([...existingSnapshots, addTimestamp(parsedSnapshot)]);
  };

  // Start snapshot fetching
  useEffect(updateSnapshots, [count]);

  // First snapshot data - used to test if first snapshot has been fetched - populates the metrics dropdown.
  const firstSnapshot: Snapshot = snapshots[0];

  return {
    firstSnapshot,
    snapshots,
    setSnapshots,
    updateSnapshots,
    resetSnapshots,
  };
};

export default useSnapshots;

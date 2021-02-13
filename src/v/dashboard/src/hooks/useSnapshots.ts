import React, { useState, useEffect } from "react";

// Libs
// @ts-ignore
import parsePrometheusTextFormat from "parse-prometheus-text-format";

// Utils
import { Metric, Snapshot } from "../types";
import getRandomInt from "utils/getRandomInt";

const useSnapshots = (
  limit: number,
  count: number,
  setCount: (count: number) => void,
  useTestData: boolean = false // NOTE 0 - change this to false
) => {
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
    fetch(`/metrics`)
      .then((response) => response.text())
      .then((data) => {
        // Parse the data
        const parsedSnapshot = parsePrometheusTextFormat(data);
        // Append new snapshot + add timestamp
        const existingSnapshots =
          snapshots.length < limit
            ? snapshots
            : snapshots.filter((item, i) => i !== 0);

        setSnapshots([...existingSnapshots, addTimestamp(parsedSnapshot)]);
      })
      .catch((error) => console.log(error));
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

import React, { useState, useEffect } from "react";

// Utils
import { Graph } from "../types";

// Default graphs from promethus snapshots - check prometheus output + choose names
const initialGraphs: Graph[] = [
  { name: "vectorized_kafka_rpc_dispatch_handler_latency", expanded: false },
  { name: "vectorized_internal_rpc_dispatch_handler_latency", expanded: false },
  { name: "vectorized_storage_log_read_bytes", expanded: false },
  { name: "vectorized_storage_log_written_bytes", expanded: false },
];

const useGraphs = () => {
  const defaultGraphs = localStorage.getItem("defaultGraphs");
  const [graphs, setGraphs] = useState<Graph[]>(
    defaultGraphs ? JSON.parse(defaultGraphs) : initialGraphs
  );

  const updateGraphs = (data: Graph[]) => {
    setGraphs(data);
    localStorage.setItem("defaultGraphs", JSON.stringify(data));
  };

  const addGraph = (event: any) => {
    event.preventDefault();
    const value = event.target[0].value;
    const isGraphSelected = graphs.filter((item) => item.name === value).length;
    if (!isGraphSelected) {
      updateGraphs([{ name: value, expanded: false }, ...graphs]);
    }
  };

  const removeGraph = (name: string) => {
    updateGraphs([...graphs.filter((item: Graph) => item.name !== name)]);
  };

  const resetGraphs = () => {
    updateGraphs(initialGraphs);
  };

  const updateGraph = (name: string, data: object) => {
    const modifiedGraphs = graphs.map((graph) => {
      if (graph.name === name) {
        return { ...graph, ...data };
      } else {
        return graph;
      }
    });
    updateGraphs([...modifiedGraphs]);
  };

  return {
    graphs,
    setGraphs,
    addGraph,
    removeGraph,
    resetGraphs,
    updateGraph,
  };
};

export default useGraphs;

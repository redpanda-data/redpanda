import React from "react";
import "./GraphSelector.scss";

// Components
import IconButton from "components/shared/IconButton/IconButton";
import Select from "components/shared/Inputs/Select";

// Utils
import { Metric, Graph } from "types";
import createTitleFromName from "utils/createTitleFromName";

const GraphSelector = ({
  options,
  graphs,
  addGraph,
  resetGraphs,
}: {
  options: Metric[];
  graphs: Graph[];
  addGraph: (event: any) => void;
  resetGraphs: () => void;
}) => {
  const reset = (e: any) => {
    e.preventDefault(); // Needs this to stop form submitting
    resetGraphs();
  };

  // Only show unused options
  const filteredOptions = options.filter((metric) => {
    const isGraphSelected = graphs.filter((graph) => metric.name === graph.name)
      .length;
    return !isGraphSelected;
  });

  return (
    <form className="GraphSelector" onSubmit={addGraph}>
      <Select name="metric-selector" id="metric-selector">
        {filteredOptions.map((metric, i) => (
          <option key={i} value={metric.name}>
            {createTitleFromName(metric.name)}
          </option>
        ))}
      </Select>
      <IconButton type="add" />
      <IconButton type="reset" onClick={reset} />
    </form>
  );
};

export default GraphSelector;

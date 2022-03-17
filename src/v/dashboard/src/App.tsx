import React from "react";
import "./App.scss";

// Components
import Logo from "components/shared/Logo";
import TimeseriesChart from "components/shared/TimeseriesChart/TimeseriesChart";
import IntervalSelector from "components/shared/IntervalSelector";
import TimeWindowSelector from "components/shared/TimeWindowSelector";
import GraphSelector from "components/shared/GraphSelector/GraphSelector";
import IconButton from "components/shared/IconButton/IconButton";

// Hooks
import useGraphs from "hooks/useGraphs";
import useInterval from "hooks/useInterval";
import useSnapshots from "hooks/useSnapshots";

function App() {
  // Default graphs from promethus snapshots - check prometheus output + choose names
  const {
    graphs,
    addGraph,
    removeGraph,
    resetGraphs,
    updateGraph,
  } = useGraphs();

  // Snapshot time interval
  const { 
    limit, 
    count, 
    setCount, 
    interval, 
    changeInterval, 
    timeWindow, 
    changeTimeWindow 
  } = useInterval();

  // Snapshot data
  const { 
    firstSnapshot, 
    snapshots, 
    resetSnapshots 
  } = useSnapshots(limit, count, setCount);

  return (
    <div className="App">
      <header className="container">
        <div>
          <Logo count={count} />
        </div>

        <div>
          <dt>Current Node</dt>
          <dd>
            {window.location.hostname}:{window.location.port}
          </dd>
        </div>

        <div>
          <dt>Time Window</dt>
          <dd>
            <TimeWindowSelector
              onChange={changeTimeWindow}
              defaultValue={timeWindow}
              value={timeWindow}
            />
          </dd>
        </div>

        <div>
          <dt>Add graph</dt>
          <dd>
            <GraphSelector
              options={firstSnapshot ? firstSnapshot.data : []}
              graphs={graphs}
              addGraph={addGraph}
              resetGraphs={resetGraphs}
            />
          </dd>
        </div>

        <div>
          <dt>Reset Graphs</dt>
          <dd>
            <IconButton type="reset" onClick={resetGraphs} />
          </dd>
        </div>
        
        <div>
          <dt>Clear Data</dt>
          <dd>
            <IconButton type="remove" onClick={resetSnapshots} />
          </dd>
        </div>

      </header>

      <main className="container">
        {graphs.map((graph) => (
          <TimeseriesChart
            key={graph.name}
            graph={graph}
            snapshots={snapshots}
            remove={removeGraph}
            update={updateGraph}
            interval={interval}
            timeWindow={timeWindow}
          />
        ))}
      </main>

      <footer className="container">
        <ul>
          {[
            {
              url: "https://vectorized.io/redpanda",
              title: "Enterprise License",
              target: "_blank",
            },
            {
              url: "https://vectorized.io/docs",
              title: "Documentation",
              target: "_blank",
            },
            {
              url: "https://vectorized.io/contact",
              title: "Contact",
              target: "_blank",
            },
            {
              url: "https://github.com/redpanda-data/redpanda",
              title: "Github",
              target: "_blank",
            },
            {
              url:
                "https://join.slack.com/t/vectorizedcommunity/shared_invite/zt-i4adpxqv-g7Zr~bxwQ3ohYPyMIG6Y6w",
              title: "Slack",
              target: "_blank",
            },
            {
              url: "https://twitter.com/VectorizedIO",
              title: "Twitter",
              target: "_blank",
            },
          ].map((item, i) => (
            <li key={i}>
              <a href={item.url} target={item.target}>
                {item.title}
              </a>
            </li>
          ))}
        </ul>
      </footer>
    </div>
  );
}

export default App;

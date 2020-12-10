export interface MetricPoint {
  value: string;
}

export interface Metric {
  name: string;
  help: string; // This is the title
  type: string;
  metrics: MetricPoint[];
}

export interface Snapshot {
  timestamp: number;
  data: Metric[];
}

export interface GraphPoint {
  x: number | Date;
  y: number;
}

export interface Graph {
  name: string;
  expanded: boolean;
}

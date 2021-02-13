import React, { useState, useEffect } from "react";

// Time Converters
const intervalFromTimeWindow = (timeWindow: number, limit: number) =>
  timeWindow / limit;
const timeWindowFromInterval = (interval: number, limit: number) =>
  interval * limit;

// Initial Values - all time is in Milliseconds (ms)
const initialCount: number = 0;
const initialLimit: number = 500;
const initialTimeWindow: number = 1000 * 60 * 15; // 15 minutes
const initialInterval: number = intervalFromTimeWindow(
  initialTimeWindow,
  initialLimit
);

// User set defaults
const defaultInterval = localStorage.getItem("defaultInterval");

const useInterval = () => {
  const [count, setCount] = useState(initialCount);
  const [limit, setLimit] = useState(initialLimit);

  const [interval, setInterval] = useState(
    defaultInterval ? parseInt(JSON.parse(defaultInterval)) : initialInterval
  );

  const [timeWindow, setTimeWindow] = useState(
    timeWindowFromInterval(interval, limit)
  );

  const updateInterval = (data: number) => {
    setInterval(data);
    localStorage.setItem("defaultInterval", JSON.stringify(data));
  };

  const changeInterval = (event: any) => {
    const interval = parseInt(event?.target?.value);
    setTimeWindow(timeWindowFromInterval(interval, limit));
    updateInterval(interval);
  };

  const changeTimeWindow = (event: any) => {
    const timeWindow = parseInt(event?.target?.value);
    setTimeWindow(timeWindow);
    updateInterval(intervalFromTimeWindow(timeWindow, limit));
  };

  const resetInterval = () => {
    updateInterval(initialInterval);
  };

  useEffect(() => {
    const timer = setTimeout(() => setCount(count + 1), interval);
    return () => clearTimeout(timer);
  }, [interval, count, setCount]);

  return {
    limit,
    setLimit,

    count,
    setCount,

    interval,
    setInterval,
    changeInterval,
    resetInterval,

    timeWindow,
    changeTimeWindow,
  };
};

export default useInterval;
